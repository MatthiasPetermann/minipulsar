package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"minipulsar/internal/protocol"
	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

const socketTestTimeout = 2 * time.Second

func TestSocketConnectAndPing(t *testing.T) {
	b, addr := startSocketTestBroker(t)
	_ = b
	conn := dialSocketTestBroker(t, addr)

	writeSocketCommand(t, conn, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_CONNECT.Enum(),
		Connect: &pulsar.CommandConnect{
			ClientVersion:   proto.String("socket-test"),
			ProtocolVersion: proto.Int32(21),
		},
	})
	connected, _ := readSocketFrame(t, conn)
	if connected.GetType() != pulsar.BaseCommand_CONNECTED {
		t.Fatalf("expected CONNECTED, got %s", connected.GetType())
	}
	if connected.GetConnected().GetServerVersion() != b.cfg.ServerVersion {
		t.Fatalf("unexpected server version: %q", connected.GetConnected().GetServerVersion())
	}
	if connected.GetConnected().GetProtocolVersion() != 21 {
		t.Fatalf("unexpected protocol version: %d", connected.GetConnected().GetProtocolVersion())
	}

	writeSocketCommand(t, conn, &pulsar.BaseCommand{Type: pulsar.BaseCommand_PING.Enum(), Ping: &pulsar.CommandPing{}})
	pong, _ := readSocketFrame(t, conn)
	if pong.GetType() != pulsar.BaseCommand_PONG {
		t.Fatalf("expected PONG, got %s", pong.GetType())
	}
}

func TestSocketUnsupportedCommandReturnsError(t *testing.T) {
	_, addr := startSocketTestBroker(t)
	conn := dialSocketTestBroker(t, addr)

	unsupported := pulsar.BaseCommand_Type(99)
	writeSocketCommand(t, conn, &pulsar.BaseCommand{Type: unsupported.Enum()})
	response, _ := readSocketFrame(t, conn)
	if response.GetType() != pulsar.BaseCommand_ERROR {
		t.Fatalf("expected ERROR, got %s", response.GetType())
	}
	if response.GetError().GetError() != pulsar.ServerError_UnsupportedVersionError {
		t.Fatalf("unexpected error code: %s", response.GetError().GetError())
	}
}

func TestShutdownClosesListenerAndConnections(t *testing.T) {
	b := New(openTestStore(t), Config{ReadTimeout: socketTestTimeout, WriteTimeout: socketTestTimeout})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- b.serveListener(listener, false) }()
	conn := dialSocketTestBroker(t, listener.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), socketTestTimeout)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("serve after shutdown: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(socketTestTimeout))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected connection to close during shutdown")
	}
}

func TestSocketProducerSubscribeFlowAndAck(t *testing.T) {
	b, addr := startSocketTestBroker(t)
	consumer := dialSocketTestBroker(t, addr)
	producer := dialSocketTestBroker(t, addr)
	const (
		topic        = "persistent://public/default/socket-test"
		subscription = "socket-sub"
		consumerID   = 7
		producerID   = 9
		sequenceID   = 11
	)

	connectSocketClient(t, consumer)
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &pulsar.CommandSubscribe{
			Topic:           proto.String(topic),
			Subscription:    proto.String(subscription),
			SubType:         pulsar.CommandSubscribe_Shared.Enum(),
			ConsumerId:      proto.Uint64(consumerID),
			RequestId:       proto.Uint64(1),
			InitialPosition: pulsar.CommandSubscribe_Earliest.Enum(),
		},
	})
	subscribed, _ := readSocketFrame(t, consumer)
	if subscribed.GetType() != pulsar.BaseCommand_SUCCESS || subscribed.GetSuccess().GetRequestId() != 1 {
		t.Fatalf("unexpected subscribe response: %s", subscribed)
	}
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_FLOW.Enum(),
		Flow: &pulsar.CommandFlow{ConsumerId: proto.Uint64(consumerID), MessagePermits: proto.Uint32(1)},
	})

	connectSocketClient(t, producer)
	writeSocketCommand(t, producer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PRODUCER.Enum(),
		Producer: &pulsar.CommandProducer{
			Topic:        proto.String(topic),
			ProducerId:   proto.Uint64(producerID),
			RequestId:    proto.Uint64(2),
			ProducerName: proto.String("socket-producer"),
		},
	})
	producerSuccess, _ := readSocketFrame(t, producer)
	if producerSuccess.GetType() != pulsar.BaseCommand_PRODUCER_SUCCESS || producerSuccess.GetProducerSuccess().GetRequestId() != 2 {
		t.Fatalf("unexpected producer response: %s", producerSuccess)
	}

	writeSocketSend(t, producer, producerID, sequenceID, []byte("socket payload"))
	receipt, _ := readSocketFrame(t, producer)
	if receipt.GetType() != pulsar.BaseCommand_SEND_RECEIPT || receipt.GetSendReceipt().GetSequenceId() != sequenceID {
		t.Fatalf("unexpected send receipt: %s", receipt)
	}

	message, payload := readSocketFrame(t, consumer)
	if message.GetType() != pulsar.BaseCommand_MESSAGE {
		t.Fatalf("expected MESSAGE, got %s", message.GetType())
	}
	if message.GetMessage().GetConsumerId() != consumerID || !bytes.Equal(payload, []byte("socket payload")) {
		t.Fatalf("unexpected delivery: consumer=%d payload=%q", message.GetMessage().GetConsumerId(), payload)
	}
	entryID := message.GetMessage().GetMessageId().GetEntryId()
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_ACK.Enum(),
		Ack: &pulsar.CommandAck{
			ConsumerId: proto.Uint64(consumerID),
			AckType:    pulsar.CommandAck_Individual.Enum(),
			MessageId:  []*pulsar.MessageIdData{{LedgerId: proto.Uint64(0), EntryId: proto.Uint64(entryID)}},
		},
	})

	deadline := time.Now().Add(socketTestTimeout)
	for {
		stats, err := b.store.StatsSnapshot(0)
		if err != nil {
			t.Fatalf("read store stats: %v", err)
		}
		if stats.Pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ack did not clear pending message: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSocketSubscriptionStartSeekAndRedeliver(t *testing.T) {
	b, addr := startSocketTestBroker(t)
	const (
		topic        = "persistent://public/default/socket-positioning"
		subscription = "positioned-sub"
		consumerID   = 17
	)
	first := &storage.Message{Topic: topic, Payload: []byte("first")}
	second := &storage.Message{Topic: topic, Payload: []byte("second")}
	if err := b.store.InsertMessage(first); err != nil {
		t.Fatalf("insert first: %v", err)
	}
	if err := b.store.InsertMessage(second); err != nil {
		t.Fatalf("insert second: %v", err)
	}

	consumer := dialSocketTestBroker(t, addr)
	connectSocketClient(t, consumer)
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &pulsar.CommandSubscribe{
			Topic:          proto.String(topic),
			Subscription:   proto.String(subscription),
			SubType:        pulsar.CommandSubscribe_Exclusive.Enum(),
			ConsumerId:     proto.Uint64(consumerID),
			RequestId:      proto.Uint64(1),
			StartMessageId: &pulsar.MessageIdData{LedgerId: proto.Uint64(0), EntryId: proto.Uint64(uint64(second.ID))},
		},
	})
	if response, _ := readSocketFrame(t, consumer); response.GetType() != pulsar.BaseCommand_SUCCESS {
		t.Fatalf("expected subscribe success, got %s", response.GetType())
	}
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_GET_LAST_MESSAGE_ID.Enum(),
		GetLastMessageId: &pulsar.CommandGetLastMessageId{
			ConsumerId: proto.Uint64(consumerID), RequestId: proto.Uint64(3),
		},
	})
	last, _ := readSocketFrame(t, consumer)
	if last.GetType() != pulsar.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE || last.GetGetLastMessageIdResponse().GetLastMessageId().GetEntryId() != uint64(second.ID) {
		t.Fatalf("unexpected last message response: %s", last)
	}
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_FLOW.Enum(),
		Flow: &pulsar.CommandFlow{ConsumerId: proto.Uint64(consumerID), MessagePermits: proto.Uint32(1)},
	})
	message, payload := readSocketFrame(t, consumer)
	if message.GetType() != pulsar.BaseCommand_MESSAGE || !bytes.Equal(payload, second.Payload) {
		t.Fatalf("start_message_id delivered %s with payload %q", message.GetType(), payload)
	}

	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type:                            pulsar.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum(),
		RedeliverUnacknowledgedMessages: &pulsar.CommandRedeliverUnacknowledgedMessages{ConsumerId: proto.Uint64(consumerID)},
	})
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_FLOW.Enum(),
		Flow: &pulsar.CommandFlow{ConsumerId: proto.Uint64(consumerID), MessagePermits: proto.Uint32(1)},
	})
	message, payload = readSocketFrame(t, consumer)
	if message.GetType() != pulsar.BaseCommand_MESSAGE || !bytes.Equal(payload, second.Payload) {
		t.Fatalf("redelivery delivered %s with payload %q", message.GetType(), payload)
	}

	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_ACK.Enum(),
		Ack: &pulsar.CommandAck{
			ConsumerId: proto.Uint64(consumerID), AckType: pulsar.CommandAck_Individual.Enum(),
			MessageId: []*pulsar.MessageIdData{message.GetMessage().GetMessageId()},
		},
	})
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_FLOW.Enum(),
		Flow: &pulsar.CommandFlow{ConsumerId: proto.Uint64(consumerID), MessagePermits: proto.Uint32(1)},
	})
	writeSocketCommand(t, consumer, &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SEEK.Enum(),
		Seek: &pulsar.CommandSeek{
			ConsumerId: proto.Uint64(consumerID), RequestId: proto.Uint64(2),
			MessageId: &pulsar.MessageIdData{LedgerId: proto.Uint64(0), EntryId: proto.Uint64(uint64(first.ID))},
		},
	})

	seenSuccess := false
	seenFirst := false
	for !seenSuccess || !seenFirst {
		response, body := readSocketFrame(t, consumer)
		switch response.GetType() {
		case pulsar.BaseCommand_SUCCESS:
			if response.GetSuccess().GetRequestId() == 2 {
				seenSuccess = true
			}
		case pulsar.BaseCommand_MESSAGE:
			if bytes.Equal(body, first.Payload) {
				seenFirst = true
			}
		}
	}
}

func startSocketTestBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	b := New(openTestStore(t), Config{ReadTimeout: socketTestTimeout, WriteTimeout: socketTestTimeout})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var connections sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				b.handleConnection(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		connections.Wait()
	})
	return b, listener.Addr().String()
}

func dialSocketTestBroker(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, socketTestTimeout)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func connectSocketClient(t *testing.T, conn net.Conn) {
	t.Helper()
	writeSocketCommand(t, conn, &pulsar.BaseCommand{
		Type:    pulsar.BaseCommand_CONNECT.Enum(),
		Connect: &pulsar.CommandConnect{ClientVersion: proto.String("socket-test"), ProtocolVersion: proto.Int32(21)},
	})
	response, _ := readSocketFrame(t, conn)
	if response.GetType() != pulsar.BaseCommand_CONNECTED {
		t.Fatalf("expected CONNECTED, got %s", response.GetType())
	}
}

func writeSocketCommand(t *testing.T, conn net.Conn, command *pulsar.BaseCommand) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(socketTestTimeout))
	if err := protocol.WriteSimpleCommand(conn, command); err != nil {
		t.Fatalf("write %s: %v", command.GetType(), err)
	}
}

func writeSocketSend(t *testing.T, conn net.Conn, producerID, sequenceID uint64, payload []byte) {
	t.Helper()
	metadata, err := proto.Marshal(&pulsar.MessageMetadata{
		ProducerName: proto.String("socket-producer"), SequenceId: proto.Uint64(sequenceID), PublishTime: proto.Uint64(uint64(time.Now().UnixMilli())),
	})
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	var metadataSize [4]byte
	binary.BigEndian.PutUint32(metadataSize[:], uint32(len(metadata)))
	checksum := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, _ = checksum.Write(metadataSize[:])
	_, _ = checksum.Write(metadata)
	_, _ = checksum.Write(payload)
	command, err := proto.Marshal(&pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SEND.Enum(),
		Send: &pulsar.CommandSend{ProducerId: proto.Uint64(producerID), SequenceId: proto.Uint64(sequenceID)},
	})
	if err != nil {
		t.Fatalf("marshal send: %v", err)
	}
	frame := make([]byte, 4+len(command)+2+4+4+len(metadata)+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(command)))
	offset := 4
	offset += copy(frame[offset:], command)
	binary.BigEndian.PutUint16(frame[offset:], protocol.MagicMessageFormat)
	offset += 2
	binary.BigEndian.PutUint32(frame[offset:], checksum.Sum32())
	offset += 4
	offset += copy(frame[offset:], metadataSize[:])
	offset += copy(frame[offset:], metadata)
	copy(frame[offset:], payload)

	_ = conn.SetWriteDeadline(time.Now().Add(socketTestTimeout))
	if err := binary.Write(conn, binary.BigEndian, uint32(len(frame))); err != nil {
		t.Fatalf("write send size: %v", err)
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write send frame: %v", err)
	}
}

func readSocketFrame(t *testing.T, conn net.Conn) (*pulsar.BaseCommand, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(socketTestTimeout))
	var totalSize uint32
	if err := binary.Read(conn, binary.BigEndian, &totalSize); err != nil {
		t.Fatalf("read frame size: %v", err)
	}
	frame := make([]byte, totalSize)
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	commandSize := binary.BigEndian.Uint32(frame[:4])
	var command pulsar.BaseCommand
	if err := proto.Unmarshal(frame[4:4+commandSize], &command); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if command.GetType() != pulsar.BaseCommand_MESSAGE {
		return &command, nil
	}
	payloadStart := 4 + commandSize + 2 + 4
	metadataSize := binary.BigEndian.Uint32(frame[payloadStart : payloadStart+4])
	payloadStart += 4 + metadataSize
	return &command, frame[payloadStart:]
}
