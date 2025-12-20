package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"

	pulsar "minipulsar/pb"
)

const (
	// Magic value used by Pulsar to mark the start of a message metadata+payload frame
	magicMessageFormat uint16 = 0x0e01
)

// --- NEW: connection-scoped keys to avoid ID collisions across clients ---
type producerKey struct {
	conn net.Conn
	id   uint64
}

type consumerKey struct {
	conn net.Conn
	id   uint64
}

type broker struct {
	db *sql.DB

	// producers/consumers are keyed by (conn, id) to avoid collisions across connections
	producers map[producerKey]*producer
	consumers map[consumerKey]*consumer

	// subscription states keyed by (topic, subscription)
	subs map[subKey]*subState

	mu sync.RWMutex
}

type producer struct {
	id    uint64
	topic string
	conn  net.Conn
}

type consumer struct {
	id           uint64
	topic        string
	subscription string
	conn         net.Conn

	mu      sync.Mutex
	permits int
}

type storedMessage struct {
	id          int64
	topic       string
	payload     []byte
	sequenceID  uint64
	publishTime int64
}

type subKey struct {
	topic string
	name  string
}

type subState struct {
	key subKey

	mu         sync.Mutex
	consumers  []*consumer
	rr         int
	delivering bool
}

func main() {
	addr := flag.String("addr", ":6650", "listen address for Pulsar binary protocol")
	dbPath := flag.String("db", "./minipulsar.db", "path to sqlite database")
	flag.Parse()

	db, err := sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	if err := initSchema(db); err != nil {
		log.Fatalf("init db schema: %v", err)
	}

	b := &broker{
		db:        db,
		producers: make(map[producerKey]*producer),
		consumers: make(map[consumerKey]*consumer),
		subs:      make(map[subKey]*subState),
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("minipulsar listening on %s (db=%s)", *addr, *dbPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go b.handleConnection(conn)
	}
}

func initSchema(db *sql.DB) error {
	schema := `
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  payload BLOB NOT NULL,
  publish_time INTEGER NOT NULL,
  sequence_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
  topic TEXT NOT NULL,
  name  TEXT NOT NULL,
  type  TEXT NOT NULL DEFAULT 'shared',
  PRIMARY KEY (topic, name)
);

CREATE TABLE IF NOT EXISTS subscription_cursor (
  topic TEXT NOT NULL,
  name  TEXT NOT NULL,
  next_message_id INTEGER NOT NULL,
  PRIMARY KEY (topic, name),
  FOREIGN KEY (topic, name) REFERENCES subscriptions(topic, name)
);

CREATE TABLE IF NOT EXISTS subscription_pending (
  topic TEXT NOT NULL,
  name  TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  consumer_id INTEGER NOT NULL,
  delivered_at INTEGER NOT NULL,
  PRIMARY KEY (topic, name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_by_sub
  ON subscription_pending(topic, name, message_id);

CREATE INDEX IF NOT EXISTS idx_messages_by_topic
  ON messages(topic, id);
`
	_, err := db.Exec(schema)
	return err
}

func (b *broker) handleConnection(conn net.Conn) {
	defer func() {
		b.cleanupConnection(conn)
		_ = conn.Close()
	}()
	remote := conn.RemoteAddr().String()
	log.Printf("new connection from %s", remote)

	for {
		if err := b.handleFrame(conn); err != nil {
			if err == io.EOF {
				log.Printf("connection %s closed", remote)
			} else {
				log.Printf("connection %s error: %v", remote, err)
			}
			return
		}
	}
}

func (b *broker) cleanupConnection(conn net.Conn) {
	// Remove producers/consumers bound to this conn; also clear pending for those consumers.
	var consumerKeys []consumerKey
	var producerKeys []producerKey

	b.mu.RLock()
	for k := range b.consumers {
		if k.conn == conn {
			consumerKeys = append(consumerKeys, k)
		}
	}
	for k := range b.producers {
		if k.conn == conn {
			producerKeys = append(producerKeys, k)
		}
	}
	b.mu.RUnlock()

	for _, k := range producerKeys {
		b.mu.Lock()
		delete(b.producers, k)
		b.mu.Unlock()
	}

	for _, k := range consumerKeys {
		// Best-effort close semantics
		b.removeConsumer(k)
	}
}

func (b *broker) handleFrame(conn net.Conn) error {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return err
	}
	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	if totalSize == 0 {
		return fmt.Errorf("invalid frame size 0")
	}
	if totalSize > 10*1024*1024 {
		return fmt.Errorf("frame too big: %d bytes", totalSize)
	}

	frame := make([]byte, totalSize)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return err
	}

	r := bytes.NewReader(frame)

	// First 4 bytes in frame: commandSize
	var cmdSizeBuf [4]byte
	if _, err := io.ReadFull(r, cmdSizeBuf[:]); err != nil {
		return fmt.Errorf("read command size: %w", err)
	}
	cmdSize := binary.BigEndian.Uint32(cmdSizeBuf[:])
	if cmdSize == 0 || int(cmdSize) > r.Len() {
		return fmt.Errorf("invalid command size %d (frame len %d)", cmdSize, r.Len())
	}

	cmdBytes := make([]byte, cmdSize)
	if _, err := io.ReadFull(r, cmdBytes); err != nil {
		return fmt.Errorf("read command: %w", err)
	}

	var base pulsar.BaseCommand
	if err := proto.Unmarshal(cmdBytes, &base); err != nil {
		return fmt.Errorf("unmarshal BaseCommand: %w", err)
	}

	switch base.GetType() {
	case pulsar.BaseCommand_CONNECT:
		return b.handleConnect(conn, &base)
	case pulsar.BaseCommand_PRODUCER:
		return b.handleProducer(conn, &base)
	case pulsar.BaseCommand_SUBSCRIBE:
		return b.handleSubscribe(conn, &base)
	case pulsar.BaseCommand_SEND:
		payloadSection, _ := io.ReadAll(r)
		return b.handleSend(conn, &base, payloadSection)
	case pulsar.BaseCommand_FLOW:
		return b.handleFlow(conn, &base)
	case pulsar.BaseCommand_ACK:
		return b.handleAck(conn, &base)
	case pulsar.BaseCommand_PING:
		return b.handlePing(conn, &base)
	case pulsar.BaseCommand_PARTITIONED_METADATA:
		return b.handlePartitionedMetadata(conn, &base)
	case pulsar.BaseCommand_LOOKUP:
		return b.handleLookup(conn, &base)
	case pulsar.BaseCommand_CLOSE_PRODUCER:
		return b.handleCloseProducer(conn, &base)
	case pulsar.BaseCommand_CLOSE_CONSUMER:
		return b.handleCloseConsumer(conn, &base)
	default:
		log.Printf("unhandled command type: %v", base.GetType())
		return nil
	}
}

func (b *broker) handleConnect(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetConnect()
	if cmd == nil {
		return fmt.Errorf("CONNECT without payload")
	}
	log.Printf("CONNECT from client_version=%s protocol=%d", cmd.GetClientVersion(), cmd.GetProtocolVersion())

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_CONNECTED.Enum(),
		Connected: &pulsar.CommandConnected{
			ServerVersion:   proto.String("minipulsar-0.1"),
			ProtocolVersion: proto.Int32(cmd.GetProtocolVersion()),
			MaxMessageSize:  proto.Int32(5 * 1024 * 1024),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) handlePartitionedMetadata(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetPartitionMetadata()
	if cmd == nil {
		return fmt.Errorf("PARTITIONED_METADATA without payload")
	}
	log.Printf("PARTITIONED_METADATA topic=%s", cmd.GetTopic())

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PARTITIONED_METADATA_RESPONSE.Enum(),
		PartitionMetadataResponse: &pulsar.CommandPartitionedTopicMetadataResponse{
			RequestId:  proto.Uint64(cmd.GetRequestId()),
			Response:   pulsar.CommandPartitionedTopicMetadataResponse_Success.Enum(),
			Partitions: proto.Uint32(0),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) handleLookup(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetLookupTopic()
	if cmd == nil {
		return fmt.Errorf("LOOKUP without CommandLookupTopic")
	}
	log.Printf("LOOKUP topic=%s", cmd.GetTopic())

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_LOOKUP_RESPONSE.Enum(),
		LookupTopicResponse: &pulsar.CommandLookupTopicResponse{
			RequestId:        proto.Uint64(cmd.GetRequestId()),
			Response:         pulsar.CommandLookupTopicResponse_Connect.Enum(),
			BrokerServiceUrl: proto.String("pulsar://localhost:6650"),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) handleProducer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetProducer()
	if cmd == nil {
		return fmt.Errorf("PRODUCER without payload")
	}
	topic := cmd.GetTopic()
	producerID := cmd.GetProducerId()
	if topic == "" {
		return fmt.Errorf("producer without topic")
	}
	name := cmd.GetProducerName()
	if name == "" {
		name = fmt.Sprintf("minipulsar-producer-%d", producerID)
	}

	key := producerKey{conn: conn, id: producerID}

	b.mu.Lock()
	b.producers[key] = &producer{
		id:    producerID,
		topic: topic,
		conn:  conn,
	}
	b.mu.Unlock()

	log.Printf("PRODUCER id=%d topic=%s name=%s", producerID, topic, name)

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PRODUCER_SUCCESS.Enum(),
		ProducerSuccess: &pulsar.CommandProducerSuccess{
			RequestId:    proto.Uint64(cmd.GetRequestId()),
			ProducerName: proto.String(name),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) ensureSubscription(topic, name string) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscriptions(topic, name, type) VALUES(?, ?, 'shared')",
		topic, name,
	); err != nil {
		return err
	}

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscription_cursor(topic, name, next_message_id) VALUES(?, ?, 1)",
		topic, name,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *broker) getOrCreateSubState(topic, name string) *subState {
	key := subKey{topic: topic, name: name}

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.subs[key]
	if s == nil {
		s = &subState{key: key}
		b.subs[key] = s
	}
	return s
}

func (b *broker) handleSubscribe(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetSubscribe()
	if cmd == nil {
		return fmt.Errorf("SUBSCRIBE without payload")
	}
	topic := cmd.GetTopic()
	sub := cmd.GetSubscription()
	consumerID := cmd.GetConsumerId()
	if topic == "" || sub == "" {
		return fmt.Errorf("invalid subscribe: empty topic or subscription")
	}

	if err := b.ensureSubscription(topic, sub); err != nil {
		return fmt.Errorf("ensure subscription: %w", err)
	}

	c := &consumer{
		id:           consumerID,
		topic:        topic,
		subscription: sub,
		conn:         conn,
	}

	key := consumerKey{conn: conn, id: consumerID}

	// Register consumer (scoped to this conn)
	b.mu.Lock()
	if old := b.consumers[key]; old != nil {
		b.mu.Unlock()
		b.removeConsumer(key) // evict old on same conn+id (best-effort)
		b.mu.Lock()
	}
	b.consumers[key] = c
	b.mu.Unlock()

	// Attach to subscription state
	s := b.getOrCreateSubState(topic, sub)
	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()

	log.Printf("SUBSCRIBE consumer=%d topic=%s subscription=%s", consumerID, topic, sub)

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) handleSend(conn net.Conn, base *pulsar.BaseCommand, payloadSection []byte) error {
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
	if magic != magicMessageFormat {
		return fmt.Errorf("unexpected magic 0x%x", magic)
	}

	// Skip checksum (CRC32C)
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

	msg := storedMessage{
		topic:       p.topic,
		payload:     payload,
		sequenceID:  meta.GetSequenceId(),
		publishTime: int64(meta.GetPublishTime()),
	}
	if err := b.insertMessage(&msg); err != nil {
		return fmt.Errorf("insert message: %w", err)
	}

	b.kickTopic(p.topic)

	log.Printf("SEND topic=%s producer=%d msgID=%d size=%d", p.topic, producerID, msg.id, len(payload))

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SEND_RECEIPT.Enum(),
		SendReceipt: &pulsar.CommandSendReceipt{
			ProducerId: proto.Uint64(producerID),
			SequenceId: proto.Uint64(cmd.GetSequenceId()),
			MessageId: &pulsar.MessageIdData{
				LedgerId: proto.Uint64(0),
				EntryId:  proto.Uint64(uint64(msg.id)),
			},
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) kickTopic(topic string) {
	// snapshot subscription states for topic
	var subs []*subState
	b.mu.RLock()
	for k, s := range b.subs {
		if k.topic == topic {
			subs = append(subs, s)
		}
	}
	b.mu.RUnlock()

	for _, s := range subs {
		b.maybeStartSubDelivery(s)
	}
}

func (b *broker) maybeStartSubDelivery(s *subState) {
	s.mu.Lock()
	// already running?
	if s.delivering {
		s.mu.Unlock()
		return
	}
	// do we have any consumer with permits?
	ready := false
	for _, c := range s.consumers {
		c.mu.Lock()
		p := c.permits
		c.mu.Unlock()
		if p > 0 {
			ready = true
			break
		}
	}
	if !ready {
		s.mu.Unlock()
		return
	}
	s.delivering = true
	s.mu.Unlock()

	go b.deliveryLoopShared(s)
}

func (s *subState) nextConsumerWithPermits() *consumer {
	n := len(s.consumers)
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		idx := (s.rr + i) % n
		c := s.consumers[idx]
		c.mu.Lock()
		p := c.permits
		c.mu.Unlock()
		if p > 0 {
			s.rr = (idx + 1) % n
			return c
		}
	}
	return nil
}

func (b *broker) deliveryLoopShared(s *subState) {
	defer func() {
		s.mu.Lock()
		s.delivering = false
		s.mu.Unlock()
	}()

	// Simple batching to keep latency OK and DB load reasonable
	const maxBatch = 200

	for {
		// Pick consumer with permits
		s.mu.Lock()
		c := s.nextConsumerWithPermits()
		s.mu.Unlock()
		if c == nil {
			return
		}

		c.mu.Lock()
		permits := c.permits
		c.mu.Unlock()
		if permits <= 0 {
			continue
		}
		limit := permits
		if limit > maxBatch {
			limit = maxBatch
		}

		nextID, err := b.dbGetCursor(s.key.topic, s.key.name)
		if err != nil {
			log.Printf("deliver cursor error topic=%s sub=%s: %v", s.key.topic, s.key.name, err)
			return
		}

		msgs, err := b.dbFetchDeliverable(s.key.topic, s.key.name, nextID, limit)
		if err != nil {
			log.Printf("deliver fetch error topic=%s sub=%s: %v", s.key.topic, s.key.name, err)
			return
		}
		if len(msgs) == 0 {
			return
		}

		now := time.Now().UnixMilli()

		for _, m := range msgs {
			// Mark pending first; if send fails, we revert it.
			if err := b.dbInsertPending(s.key.topic, s.key.name, m.id, int64(c.id), now); err != nil {
				log.Printf("pending insert error topic=%s sub=%s msg=%d: %v", s.key.topic, s.key.name, m.id, err)
				return
			}

			if err := writeMessageFrame(c.conn, c.id, &m); err != nil {
				_ = b.dbDeletePending(s.key.topic, s.key.name, m.id)
				log.Printf("deliver write error consumer=%d: %v", c.id, err)
				return
			}

			c.mu.Lock()
			if c.permits > 0 {
				c.permits--
			}
			c.mu.Unlock()
		}
	}
}

func (b *broker) handleFlow(conn net.Conn, base *pulsar.BaseCommand) error {
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

	// Start / wake subscription delivery
	s := b.getOrCreateSubState(c.topic, c.subscription)
	b.maybeStartSubDelivery(s)
	return nil
}

func (b *broker) handleAck(conn net.Conn, base *pulsar.BaseCommand) error {
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
		// Consumer already gone
		return nil
	}

	topic := c.topic
	sub := c.subscription

	ids := cmd.GetMessageId()
	if len(ids) == 0 {
		log.Printf("ACK consumer=%d type=%v (#ids=0)", consumerID, cmd.GetAckType())
		return nil
	}

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	switch cmd.GetAckType() {
	case pulsar.CommandAck_Individual:
		for _, mid := range ids {
			msgID := int64(mid.GetEntryId())
			if _, err := tx.Exec(
				"DELETE FROM subscription_pending WHERE topic=? AND name=? AND message_id=?",
				topic, sub, msgID,
			); err != nil {
				return err
			}
		}

	case pulsar.CommandAck_Cumulative:
		// Pulsar semantics: all messages <= entryId are acked
		if len(ids) != 1 {
			return fmt.Errorf("invalid cumulative ack: %d ids", len(ids))
		}
		upto := int64(ids[0].GetEntryId())
		if _, err := tx.Exec(
			"DELETE FROM subscription_pending WHERE topic=? AND name=? AND message_id <= ?",
			topic, sub, upto,
		); err != nil {
			return err
		}

	default:
		log.Printf("ACK consumer=%d unsupported type=%v", consumerID, cmd.GetAckType())
	}

	// Advance cursor after ack
	if err := b.txAdvanceCursor(tx, topic, sub); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("ACK consumer=%d type=%v (#ids=%d) topic=%s sub=%s",
		consumerID, cmd.GetAckType(), len(ids), topic, sub)

	// Try delivering more if permits allow
	s := b.getOrCreateSubState(topic, sub)
	b.maybeStartSubDelivery(s)

	return nil
}

func (b *broker) handlePing(conn net.Conn, base *pulsar.BaseCommand) error {
	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PONG.Enum(),
		Pong: &pulsar.CommandPong{},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) insertMessage(msg *storedMessage) error {
	if msg.publishTime == 0 {
		msg.publishTime = time.Now().UnixMilli()
	}
	res, err := b.db.Exec(
		"INSERT INTO messages(topic, payload, publish_time, sequence_id) VALUES(?, ?, ?, ?)",
		msg.topic, msg.payload, msg.publishTime, msg.sequenceID,
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	msg.id = id
	return nil
}

func (b *broker) fetchMessages(topic string, fromID int64, limit int) ([]storedMessage, error) {
	rows, err := b.db.Query(
		"SELECT id, topic, payload, publish_time, sequence_id FROM messages WHERE topic = ? AND id >= ? ORDER BY id LIMIT ?",
		topic, fromID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []storedMessage
	for rows.Next() {
		var m storedMessage
		if err := rows.Scan(&m.id, &m.topic, &m.payload, &m.publishTime, &m.sequenceID); err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

func (b *broker) dbGetCursor(topic, sub string) (int64, error) {
	var next int64
	err := b.db.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE topic=? AND name=?",
		topic, sub,
	).Scan(&next)
	if err == sql.ErrNoRows {
		// Should not happen due to ensureSubscription; create lazily.
		if err2 := b.ensureSubscription(topic, sub); err2 != nil {
			return 0, err2
		}
		return 1, nil
	}
	return next, err
}

func (b *broker) dbFetchDeliverable(topic, sub string, fromID int64, limit int) ([]storedMessage, error) {
	rows, err := b.db.Query(
		`SELECT m.id, m.topic, m.payload, m.publish_time, m.sequence_id
		 FROM messages m
		 WHERE m.topic = ?
		   AND m.id >= ?
		   AND NOT EXISTS (
			 SELECT 1 FROM subscription_pending p
			 WHERE p.topic = ? AND p.name = ? AND p.message_id = m.id
		   )
		 ORDER BY m.id
		 LIMIT ?`,
		topic, fromID, topic, sub, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []storedMessage
	for rows.Next() {
		var m storedMessage
		if err := rows.Scan(&m.id, &m.topic, &m.payload, &m.publishTime, &m.sequenceID); err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

func (b *broker) dbInsertPending(topic, sub string, msgID int64, consumerID int64, deliveredAt int64) error {
	_, err := b.db.Exec(
		"INSERT OR IGNORE INTO subscription_pending(topic, name, message_id, consumer_id, delivered_at) VALUES(?,?,?,?,?)",
		topic, sub, msgID, consumerID, deliveredAt,
	)
	return err
}

func (b *broker) dbDeletePending(topic, sub string, msgID int64) error {
	_, err := b.db.Exec(
		"DELETE FROM subscription_pending WHERE topic=? AND name=? AND message_id=?",
		topic, sub, msgID,
	)
	return err
}

func (b *broker) txAdvanceCursor(tx *sql.Tx, topic, sub string) error {
	// Read current cursor
	var cur int64
	if err := tx.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE topic=? AND name=?",
		topic, sub,
	).Scan(&cur); err != nil {
		if err == sql.ErrNoRows {
			// create cursor row
			if _, err2 := tx.Exec(
				"INSERT OR IGNORE INTO subscription_cursor(topic,name,next_message_id) VALUES(?,?,1)",
				topic, sub,
			); err2 != nil {
				return err2
			}
			cur = 1
		} else {
			return err
		}
	}

	// Move forward while:
	// - message exists at cur
	// - and it's not pending
	for {
		var exists int
		if err := tx.QueryRow(
			"SELECT 1 FROM messages WHERE topic=? AND id=? LIMIT 1",
			topic, cur,
		).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				break
			}
			return err
		}

		var pending int
		if err := tx.QueryRow(
			"SELECT 1 FROM subscription_pending WHERE topic=? AND name=? AND message_id=? LIMIT 1",
			topic, sub, cur,
		).Scan(&pending); err == nil {
			// still pending -> stop
			break
		} else if err != sql.ErrNoRows {
			return err
		}

		// not pending -> advance
		cur++
	}

	_, err := tx.Exec(
		"UPDATE subscription_cursor SET next_message_id=? WHERE topic=? AND name=?",
		cur, topic, sub,
	)
	return err
}

func writeSimpleCommand(w io.Writer, cmd *pulsar.BaseCommand) error {
	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	totalSize := uint32(4 + len(cmdBytes))
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, totalSize); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(cmdBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(cmdBytes); err != nil {
		return err
	}

	_, err = w.Write(buf.Bytes())
	return err
}

func writeMessageFrame(w io.Writer, consumerID uint64, msg *storedMessage) error {
	base := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_MESSAGE.Enum(),
		Message: &pulsar.CommandMessage{
			ConsumerId: proto.Uint64(consumerID),
			MessageId: &pulsar.MessageIdData{
				LedgerId: proto.Uint64(0),
				EntryId:  proto.Uint64(uint64(msg.id)),
			},
		},
	}
	cmdBytes, err := proto.Marshal(base)
	if err != nil {
		return fmt.Errorf("marshal base message: %w", err)
	}

	if msg.publishTime == 0 {
		msg.publishTime = time.Now().UnixMilli()
	}

	meta := &pulsar.MessageMetadata{
		ProducerName: proto.String("minipulsar"),
		SequenceId:   proto.Uint64(msg.sequenceID),
		PublishTime:  proto.Uint64(uint64(msg.publishTime)),
	}
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	var metaSizeBuf [4]byte
	binary.BigEndian.PutUint32(metaSizeBuf[:], uint32(len(metaBytes)))

	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc.Write(metaSizeBuf[:])
	crc.Write(metaBytes)
	crc.Write(msg.payload)
	checksum := crc.Sum32()

	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, uint32(len(cmdBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(cmdBytes); err != nil {
		return err
	}

	if err := binary.Write(&buf, binary.BigEndian, magicMessageFormat); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, checksum); err != nil {
		return err
	}
	if _, err := buf.Write(metaSizeBuf[:]); err != nil {
		return err
	}
	if _, err := buf.Write(metaBytes); err != nil {
		return err
	}
	if _, err := buf.Write(msg.payload); err != nil {
		return err
	}

	inner := buf.Bytes()
	totalSize := uint32(len(inner))

	var frame bytes.Buffer
	if err := binary.Write(&frame, binary.BigEndian, totalSize); err != nil {
		return err
	}
	if _, err := frame.Write(inner); err != nil {
		return err
	}

	_, err = w.Write(frame.Bytes())
	return err
}

func (b *broker) handleCloseProducer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetCloseProducer()
	if cmd == nil {
		return fmt.Errorf("CLOSE_PRODUCER without payload")
	}

	key := producerKey{conn: conn, id: cmd.GetProducerId()}

	log.Printf("CLOSE_PRODUCER producer=%d", cmd.GetProducerId())

	b.mu.Lock()
	delete(b.producers, key)
	b.mu.Unlock()

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) handleCloseConsumer(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetCloseConsumer()
	if cmd == nil {
		return fmt.Errorf("CLOSE_CONSUMER without payload")
	}

	log.Printf("CLOSE_CONSUMER consumer=%d", cmd.GetConsumerId())

	b.removeConsumer(consumerKey{conn: conn, id: cmd.GetConsumerId()})

	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return writeSimpleCommand(conn, resp)
}

func (b *broker) removeConsumer(key consumerKey) {
	var c *consumer

	b.mu.Lock()
	c = b.consumers[key]
	delete(b.consumers, key)
	b.mu.Unlock()

	if c == nil {
		return
	}

	// Remove from subState list
	skey := subKey{topic: c.topic, name: c.subscription}

	b.mu.RLock()
	s := b.subs[skey]
	b.mu.RUnlock()

	if s != nil {
		s.mu.Lock()
		dst := s.consumers[:0]
		for _, x := range s.consumers {
			if x != c {
				dst = append(dst, x)
			}
		}
		s.consumers = dst
		s.mu.Unlock()
	}

	// Drop pending messages for this consumer (best-effort)
	_, _ = b.db.Exec(
		"DELETE FROM subscription_pending WHERE topic=? AND name=? AND consumer_id=?",
		c.topic, c.subscription, int64(c.id),
	)

	// Try advancing cursor
	tx, err := b.db.Begin()
	if err == nil {
		_ = b.txAdvanceCursor(tx, c.topic, c.subscription)
		_ = tx.Commit()
	}

	// Trigger delivery again (might unblock others)
	if s != nil {
		b.maybeStartSubDelivery(s)
	}
}
