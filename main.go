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

type broker struct {
	db        *sql.DB
	producers map[uint64]*producer
	consumers map[uint64]*consumer
	mu        sync.RWMutex
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

	mu            sync.Mutex
	nextMessageID int64 // sqlite row id to start from
}

type storedMessage struct {
	id          int64
	topic       string
	payload     []byte
	sequenceID  uint64
	publishTime int64
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
		producers: make(map[uint64]*producer),
		consumers: make(map[uint64]*consumer),
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
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  payload BLOB NOT NULL,
  publish_time INTEGER NOT NULL,
  sequence_id INTEGER NOT NULL
);
`
	_, err := db.Exec(schema)
	return err
}

func (b *broker) handleConnection(conn net.Conn) {
	defer conn.Close()
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

	b.mu.Lock()
	b.producers[producerID] = &producer{
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

	c := &consumer{
		id:            consumerID,
		topic:         topic,
		subscription:  sub,
		conn:          conn,
		nextMessageID: 1,
	}

	b.mu.Lock()
	b.consumers[consumerID] = c
	b.mu.Unlock()

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

	b.mu.RLock()
	p := b.producers[producerID]
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

func (b *broker) handleFlow(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetFlow()
	if cmd == nil {
		return fmt.Errorf("FLOW without payload")
	}
	consumerID := cmd.GetConsumerId()
	permits := cmd.GetMessagePermits()
	if permits == 0 {
		return nil
	}

	b.mu.RLock()
	c := b.consumers[consumerID]
	b.mu.RUnlock()
	if c == nil {
		return fmt.Errorf("FLOW for unknown consumer %d", consumerID)
	}

	log.Printf("FLOW consumer=%d permits=%d", consumerID, permits)
	return b.deliverMessages(c, int(permits))
}

func (b *broker) handleAck(conn net.Conn, base *pulsar.BaseCommand) error {
	cmd := base.GetAck()
	if cmd == nil {
		return fmt.Errorf("ACK without payload")
	}
	log.Printf("ACK consumer=%d type=%v (#ids=%d)", cmd.GetConsumerId(), cmd.GetAckType(), len(cmd.GetMessageId()))
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

func (b *broker) deliverMessages(c *consumer, limit int) error {
	c.mu.Lock()
	startID := c.nextMessageID
	c.mu.Unlock()

	msgs, err := b.fetchMessages(c.topic, startID, limit)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	for _, m := range msgs {
		if err := writeMessageFrame(c.conn, c.id, &m); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.nextMessageID = msgs[len(msgs)-1].id + 1
	c.mu.Unlock()

	log.Printf("delivered %d messages to consumer=%d topic=%s", len(msgs), c.id, c.topic)
	return nil
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

	nowMillis := time.Now().UnixMilli()
	if msg.publishTime == 0 {
		msg.publishTime = nowMillis
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
	log.Printf("CLOSE_PRODUCER producer=%d", cmd.GetProducerId())

	// Optional: interne Producer-Map aufräumen
	b.mu.Lock()
	delete(b.producers, cmd.GetProducerId())
	b.mu.Unlock()

	// Antwort: SUCCESS(request_id)
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

	// Optional: interne Consumer-Map aufräumen
	b.mu.Lock()
	delete(b.consumers, cmd.GetConsumerId())
	b.mu.Unlock()

	// Antwort: SUCCESS(request_id)
	resp := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_SUCCESS.Enum(),
		Success: &pulsar.CommandSuccess{
			RequestId: proto.Uint64(cmd.GetRequestId()),
		},
	}
	return writeSimpleCommand(conn, resp)
}
