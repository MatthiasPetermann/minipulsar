package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"minipulsar/internal/messaging"
	"minipulsar/internal/protocol"
	"minipulsar/internal/storage"
	"minipulsar/internal/topic"
	pulsar "minipulsar/pb"
)

// handleConnect processes the initial CONNECT handshake from a Pulsar client.
// It responds with protocol metadata, including max message size and server version.
func (b *Broker) handleConnect(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetConnect()
	if cmd == nil {
		return fmt.Errorf("CONNECT without payload")
	}
	if b.cfg.Messaging != nil && b.cfg.Messaging.Security != nil {
		roles, err := rolesFromConnect(cmd, b.cfg.JWTSecret)
		if err != nil {
			return fmt.Errorf("parse auth roles: %w", err)
		}
		b.mu.Lock()
		b.connRoles[conn] = roles
		b.mu.Unlock()
	}
	b.cfg.Logger.Info("CONNECT",
		"client_version", cmd.GetClientVersion(),
		"protocol_version", cmd.GetProtocolVersion(),
	)

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_CONNECTED.Enum(),
		Connected: &pulsar.CommandConnected{
			ServerVersion:   proto.String(b.cfg.ServerVersion),
			ProtocolVersion: proto.Int32(cmd.GetProtocolVersion()),
			MaxMessageSize:  proto.Int32(b.cfg.MaxMessageSize),
		},
	}
	return b.writeCommand(conn, resp)
}

// handlePartitionedMetadata responds to partition metadata requests.
// Minipulsar currently exposes topics as non-partitioned.
func (b *Broker) handlePartitionedMetadata(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetPartitionMetadata()
	if cmd == nil {
		return fmt.Errorf("PARTITIONED_METADATA without payload")
	}
	b.cfg.Logger.Info("PARTITIONED_METADATA", "topic", cmd.GetTopic())

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PARTITIONED_METADATA_RESPONSE.Enum(),
		PartitionMetadataResponse: &pulsar.CommandPartitionedTopicMetadataResponse{
			RequestId:  proto.Uint64(cmd.GetRequestId()),
			Response:   pulsar.CommandPartitionedTopicMetadataResponse_Success.Enum(),
			Partitions: proto.Uint32(0),
		},
	}
	return b.writeCommand(conn, resp)
}

// handleLookup answers broker discovery requests from Pulsar clients.
// The response tells clients where to establish the actual data connection.
func (b *Broker) handleLookup(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetLookupTopic()
	if cmd == nil {
		return fmt.Errorf("LOOKUP without CommandLookupTopic")
	}
	b.cfg.Logger.Info("LOOKUP", "topic", cmd.GetTopic())

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_LOOKUP_RESPONSE.Enum(),
		LookupTopicResponse: &pulsar.CommandLookupTopicResponse{
			RequestId:        proto.Uint64(cmd.GetRequestId()),
			Response:         pulsar.CommandLookupTopicResponse_Connect.Enum(),
			BrokerServiceUrl: proto.String(b.cfg.BrokerServiceURL),
		},
	}
	return b.writeCommand(conn, resp)
}

// handleProducer registers a producer created by the client for a topic.
// Pulsar producers are scoped per connection and referenced by producer ID.
func (b *Broker) handleProducer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetProducer()
	if cmd == nil {
		return fmt.Errorf("PRODUCER without payload")
	}
	topicName := cmd.GetTopic()
	producerID := cmd.GetProducerId()
	if topicName == "" {
		return fmt.Errorf("producer without topic")
	}
	topicInfo, err := topic.Parse(topicName)
	if err != nil {
		return err
	}
	if err := b.authorize(conn, topicInfo, messaging.ActionProduce); err != nil {
		return err
	}
	name := cmd.GetProducerName()
	if name == "" {
		name = fmt.Sprintf("minipulsar-producer-%d", producerID)
	}

	key := producerKey{conn: conn, id: producerID}

	b.mu.Lock()
	b.producers[key] = &producer{
		id:         producerID,
		topic:      topicInfo.FullName,
		persistent: topicInfo.Persistent,
		conn:       conn,
	}
	b.mu.Unlock()

	b.cfg.Logger.Info("PRODUCER",
		"producer_id", producerID,
		"topic", topicInfo.FullName,
		"name", name,
	)

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PRODUCER_SUCCESS.Enum(),
		ProducerSuccess: &pulsar.CommandProducerSuccess{
			RequestId:    proto.Uint64(cmd.GetRequestId()),
			ProducerName: proto.String(name),
		},
	}
	return b.writeCommand(conn, resp)
}

// handleSubscribe registers a consumer on a shared subscription.
// Consumers receive messages through permit-based flow control.
func (b *Broker) handleSubscribe(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetSubscribe()
	if cmd == nil {
		return fmt.Errorf("SUBSCRIBE without payload")
	}
	topicName := cmd.GetTopic()
	sub := cmd.GetSubscription()
	consumerID := cmd.GetConsumerId()
	if topicName == "" || sub == "" {
		return fmt.Errorf("invalid subscribe: empty topic or subscription")
	}
	topicInfo, err := topic.Parse(topicName)
	if err != nil {
		return err
	}
	if err := b.authorize(conn, topicInfo, messaging.ActionConsume); err != nil {
		return err
	}

	if topicInfo.Persistent {
		if err := b.store.EnsureSubscription(topicInfo.FullName, sub); err != nil {
			return fmt.Errorf("ensure subscription: %w", err)
		}
	}

	c := &consumer{
		id:           consumerID,
		uid:          b.nextUID(),
		topic:        topicInfo.FullName,
		subscription: sub,
		persistent:   topicInfo.Persistent,
		conn:         conn,
	}

	key := consumerKey{conn: conn, id: consumerID}

	// Register consumer (scoped to this conn).
	b.mu.Lock()
	if old := b.consumers[key]; old != nil {
		b.mu.Unlock()
		b.removeConsumer(key)
		b.mu.Lock()
	}
	b.consumers[key] = c
	b.mu.Unlock()

	// Attach to subscription state.
	s := b.getOrCreateSubState(topicInfo.FullName, sub, topicInfo.Persistent)
	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()

	b.cfg.Logger.Info("SUBSCRIBE",
		"consumer_id", consumerID,
		"consumer_uid", c.uid,
		"topic", topicInfo.FullName,
		"subscription", sub,
	)

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return b.writeCommand(conn, resp)
}

// handleSend stores a message from a producer and triggers delivery.
// The payload section contains Pulsar's message metadata frame.
func (b *Broker) handleSend(conn net.Conn, base *pulsar.BaseCommand, payloadSection []byte) error {
	cmd := base.GetSend()
	if cmd == nil {
		return fmt.Errorf("SEND without payload")
	}
	producerID := cmd.GetProducerId()

	key := producerKey{conn: conn, id: producerID}

	b.mu.RLock()
	p := b.producers[key]
	b.mu.RUnlock()
	if p == nil {
		return fmt.Errorf("SEND for unknown producer %d", producerID)
	}

	if len(payloadSection) == 0 {
		return fmt.Errorf("SEND without message frame")
	}
	r := bytes.NewReader(payloadSection)

	var magicBuf [2]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return fmt.Errorf("read magic: %w", err)
	}
	magic := binary.BigEndian.Uint16(magicBuf[:])
	if magic != protocol.MagicMessageFormat {
		return fmt.Errorf("unexpected magic 0x%x", magic)
	}

	// Read checksum (CRC32C).
	var checksumBuf [4]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return fmt.Errorf("read checksum: %w", err)
	}

	var metaSizeBuf [4]byte
	if _, err := io.ReadFull(r, metaSizeBuf[:]); err != nil {
		return fmt.Errorf("read metadata size: %w", err)
	}
	metaSize := binary.BigEndian.Uint32(metaSizeBuf[:])
	if metaSize == 0 || int(metaSize) > r.Len() {
		return fmt.Errorf("invalid metadata size %d", metaSize)
	}

	metaBytes := make([]byte, metaSize)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	var meta pulsar.MessageMetadata
	if err := proto.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("unmarshal metadata: %w", err)
	}

	payload, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	if b.cfg.MaxMessageSize > 0 && len(payload) > int(b.cfg.MaxMessageSize) {
		return fmt.Errorf("message payload too large: %d bytes", len(payload))
	}
	checksum := binary.BigEndian.Uint32(checksumBuf[:])
	hasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = hasher.Write(metaSizeBuf[:])
	_, _ = hasher.Write(metaBytes)
	_, _ = hasher.Write(payload)
	if checksum != hasher.Sum32() {
		return fmt.Errorf("payload checksum mismatch")
	}

	msg := storage.Message{
		Topic:       p.topic,
		Payload:     payload,
		SequenceID:  meta.GetSequenceId(),
		PublishTime: int64(meta.GetPublishTime()),
	}
	if p.persistent {
		if err := b.store.InsertMessage(&msg); err != nil {
			return fmt.Errorf("insert message: %w", err)
		}
		b.kickTopic(p.topic)
	} else {
		msg.ID = b.nextNonPersistentID(p.topic)
		b.deliverNonPersistent(p.topic, msg)
	}

	b.cfg.Logger.Info("SEND",
		"topic", p.topic,
		"producer_id", producerID,
		"message_id", msg.ID,
		"size", len(payload),
	)

	if err := b.applyBindings(p.topic, payload); err != nil {
		b.cfg.Logger.Warn("binding processing failed", "err", err, "topic", p.topic)
	}

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SEND_RECEIPT.Enum(),
		SendReceipt: &pulsar.CommandSendReceipt{
			ProducerId: proto.Uint64(producerID),
			SequenceId: proto.Uint64(cmd.GetSequenceId()),
			MessageId: &pulsar.MessageIdData{
				LedgerId: proto.Uint64(0),
				EntryId:  proto.Uint64(uint64(msg.ID)),
			},
		},
	}
	return b.writeCommand(conn, resp)
}

func (b *Broker) applyBindings(sourceTopic string, payload []byte) error {
	if b.cfg.Messaging == nil || b.cfg.Messaging.Pool == nil {
		return nil
	}
	bindings := b.cfg.Messaging.BindingsFor(sourceTopic)
	if len(bindings) == 0 {
		return nil
	}
	for _, binding := range bindings {
		ctx := messaging.FunctionContext{
			FunctionID:  binding.FunctionID,
			SourceTopic: binding.SourceTopic,
			TargetTopic: binding.TargetTopic,
		}
		output, err := b.cfg.Messaging.Pool.Execute(binding.FunctionID, payload, ctx)
		if err != nil {
			return err
		}
		targetInfo, err := topic.Parse(binding.TargetTopic)
		if err != nil {
			return fmt.Errorf("binding target invalid: %w", err)
		}
		msg := storage.Message{
			Topic:       targetInfo.FullName,
			Payload:     output,
			SequenceID:  0,
			PublishTime: time.Now().UnixMilli(),
		}
		if targetInfo.Persistent {
			if err := b.store.InsertMessage(&msg); err != nil {
				return err
			}
			b.kickTopic(targetInfo.FullName)
		} else {
			msg.ID = b.nextNonPersistentID(targetInfo.FullName)
			b.deliverNonPersistent(targetInfo.FullName, msg)
		}
	}
	return nil
}

// handleFlow applies additional permits to a consumer, enabling delivery.
// Pulsar uses permits for backpressure on shared subscriptions.
func (b *Broker) handleFlow(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetFlow()
	if cmd == nil {
		return fmt.Errorf("FLOW without payload")
	}
	consumerID := cmd.GetConsumerId()
	add := int(cmd.GetMessagePermits())
	if add <= 0 {
		return nil
	}

	key := consumerKey{conn: conn, id: consumerID}

	b.mu.RLock()
	c := b.consumers[key]
	b.mu.RUnlock()
	if c == nil {
		return fmt.Errorf("FLOW for unknown consumer %d", consumerID)
	}

	c.mu.Lock()
	c.permits += add
	c.mu.Unlock()

	// Start / wake subscription delivery.
	if c.persistent {
		s := b.getOrCreateSubState(c.topic, c.subscription, true)
		b.maybeStartSubDelivery(s)
	}
	return nil
}

// handleAck records acknowledgements for messages delivered to a consumer.
// In shared subscriptions, only individual ack is meaningful.
func (b *Broker) handleAck(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetAck()
	if cmd == nil {
		return fmt.Errorf("ACK without payload")
	}

	consumerID := cmd.GetConsumerId()
	key := consumerKey{conn: conn, id: consumerID}

	b.mu.RLock()
	c := b.consumers[key]
	b.mu.RUnlock()
	if c == nil {
		// Consumer already gone.
		return nil
	}

	topic := c.topic
	sub := c.subscription

	if !c.persistent {
		return nil
	}

	ids := cmd.GetMessageId()
	if len(ids) == 0 {
		return nil
	}

	switch cmd.GetAckType() {
	case pulsar.CommandAck_Individual:
		// delete pending only for this consumer uid (prevents cross-consumer acks)
		pendingIDs := make([]int64, 0, len(ids))
		for _, mid := range ids {
			pendingIDs = append(pendingIDs, int64(mid.GetEntryId()))
		}
		if err := b.store.AckIndividual(topic, sub, c.uid, pendingIDs); err != nil {
			return err
		}

	case pulsar.CommandAck_Cumulative:
		// Shared semantics: cumulative ack is not meaningful and breaks correctness.
		// We ignore it (or you could treat it as Individual for the given id only).
		b.cfg.Logger.Warn("ACK cumulative ignored (shared)",
			"consumer_id", consumerID,
			"topic", topic,
			"subscription", sub,
		)
		return nil

	default:
		b.cfg.Logger.Warn("ACK unsupported type",
			"consumer_id", consumerID,
			"ack_type", cmd.GetAckType(),
		)
		return nil
	}

	// Try delivering more if permits allow.
	s := b.getOrCreateSubState(topic, sub, true)
	b.maybeStartSubDelivery(s)
	return nil
}

// handlePing replies to client keepalive messages.
func (b *Broker) handlePing(conn net.Conn, base *pulsar.BaseCommand) error {
	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PONG.Enum(),
		Pong: &pulsar.CommandPong{},
	}
	return b.writeCommand(conn, resp)
}

// handleCloseProducer removes a producer from the connection scope.
func (b *Broker) handleCloseProducer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetCloseProducer()
	if cmd == nil {
		return fmt.Errorf("CLOSE_PRODUCER without payload")
	}

	key := producerKey{conn: conn, id: cmd.GetProducerId()}

	b.cfg.Logger.Info("CLOSE_PRODUCER", "producer_id", cmd.GetProducerId())

	b.mu.Lock()
	delete(b.producers, key)
	b.mu.Unlock()

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return b.writeCommand(conn, resp)
}

// handleCloseConsumer removes a consumer and triggers delivery rebalancing.
func (b *Broker) handleCloseConsumer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetCloseConsumer()
	if cmd == nil {
		return fmt.Errorf("CLOSE_CONSUMER without payload")
	}

	b.cfg.Logger.Info("CLOSE_CONSUMER", "consumer_id", cmd.GetConsumerId())

	b.removeConsumer(consumerKey{conn: conn, id: cmd.GetConsumerId()})

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return b.writeCommand(conn, resp)
}
