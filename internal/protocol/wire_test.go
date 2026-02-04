package protocol

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"google.golang.org/protobuf/proto"

	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

func TestWriteSimpleCommand(t *testing.T) {
	// Pulsar frames begin with a 4-byte length prefix followed by command metadata,
	// so we verify the prefix math and that the command type survives round-trip encoding.
	cmd := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_PING.Enum(),
	}

	var buf bytes.Buffer
	if err := WriteSimpleCommand(&buf, cmd); err != nil {
		t.Fatalf("write command: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	var totalSize uint32
	if err := binary.Read(reader, binary.BigEndian, &totalSize); err != nil {
		t.Fatalf("read total size: %v", err)
	}
	var cmdSize uint32
	if err := binary.Read(reader, binary.BigEndian, &cmdSize); err != nil {
		t.Fatalf("read cmd size: %v", err)
	}
	if totalSize != cmdSize+4 {
		t.Fatalf("unexpected total size: got %d want %d", totalSize, cmdSize+4)
	}
	cmdBytes := make([]byte, cmdSize)
	if _, err := reader.Read(cmdBytes); err != nil {
		t.Fatalf("read cmd bytes: %v", err)
	}

	var decoded pulsar.BaseCommand
	if err := proto.Unmarshal(cmdBytes, &decoded); err != nil {
		t.Fatalf("unmarshal cmd: %v", err)
	}
	if decoded.GetType() != pulsar.BaseCommand_PING {
		t.Fatalf("unexpected command type: %v", decoded.GetType())
	}
}

func TestWriteMessageFrame(t *testing.T) {
	// Pulsar message frames include a command header, magic value, checksum, and metadata,
	// so we validate each component to match the protocol's framing expectations.
	msg := storage.Message{
		ID:          42,
		Topic:       "persistent://public/default/demo",
		Payload:     []byte("payload"),
		SequenceID:  7,
		PublishTime: 1234567890,
		Properties: map[string]string{
			"region": "eu",
			"tier":   "gold",
		},
	}

	var buf bytes.Buffer
	if err := WriteMessageFrame(&buf, 99, msg); err != nil {
		t.Fatalf("write message frame: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	var totalSize uint32
	if err := binary.Read(reader, binary.BigEndian, &totalSize); err != nil {
		t.Fatalf("read total size: %v", err)
	}
	inner := make([]byte, totalSize)
	if _, err := reader.Read(inner); err != nil {
		t.Fatalf("read inner: %v", err)
	}

	innerReader := bytes.NewReader(inner)
	var cmdSize uint32
	if err := binary.Read(innerReader, binary.BigEndian, &cmdSize); err != nil {
		t.Fatalf("read cmd size: %v", err)
	}
	cmdBytes := make([]byte, cmdSize)
	if _, err := innerReader.Read(cmdBytes); err != nil {
		t.Fatalf("read cmd bytes: %v", err)
	}

	var decoded pulsar.BaseCommand
	if err := proto.Unmarshal(cmdBytes, &decoded); err != nil {
		t.Fatalf("unmarshal cmd: %v", err)
	}
	if decoded.GetType() != pulsar.BaseCommand_MESSAGE {
		t.Fatalf("unexpected command type: %v", decoded.GetType())
	}
	if decoded.GetMessage().GetConsumerId() != 99 {
		t.Fatalf("unexpected consumer id: %d", decoded.GetMessage().GetConsumerId())
	}
	if decoded.GetMessage().GetMessageId().GetEntryId() != 42 {
		t.Fatalf("unexpected entry id: %d", decoded.GetMessage().GetMessageId().GetEntryId())
	}

	var magic uint16
	if err := binary.Read(innerReader, binary.BigEndian, &magic); err != nil {
		t.Fatalf("read magic: %v", err)
	}
	if magic != MagicMessageFormat {
		t.Fatalf("unexpected magic: %x", magic)
	}

	var checksum uint32
	if err := binary.Read(innerReader, binary.BigEndian, &checksum); err != nil {
		t.Fatalf("read checksum: %v", err)
	}

	var metaSize uint32
	if err := binary.Read(innerReader, binary.BigEndian, &metaSize); err != nil {
		t.Fatalf("read meta size: %v", err)
	}
	metaBytes := make([]byte, metaSize)
	if _, err := innerReader.Read(metaBytes); err != nil {
		t.Fatalf("read meta bytes: %v", err)
	}
	payload := make([]byte, innerReader.Len())
	if _, err := innerReader.Read(payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}

	var metadata pulsar.MessageMetadata
	if err := proto.Unmarshal(metaBytes, &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata.GetSequenceId() != msg.SequenceID {
		t.Fatalf("unexpected sequence id: %d", metadata.GetSequenceId())
	}
	if metadata.GetPublishTime() != uint64(msg.PublishTime) {
		t.Fatalf("unexpected publish time: %d", metadata.GetPublishTime())
	}
	if len(metadata.GetProperties()) != len(msg.Properties) {
		t.Fatalf("unexpected properties length: %d", len(metadata.GetProperties()))
	}
	for _, kv := range metadata.GetProperties() {
		if kv == nil {
			t.Fatalf("unexpected nil property")
		}
		if msg.Properties[kv.GetKey()] != kv.GetValue() {
			t.Fatalf("unexpected property %q=%q", kv.GetKey(), kv.GetValue())
		}
	}
	if !bytes.Equal(payload, msg.Payload) {
		t.Fatalf("unexpected payload: %q", payload)
	}

	var metaSizeBuf [4]byte
	binary.BigEndian.PutUint32(metaSizeBuf[:], metaSize)
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc.Write(metaSizeBuf[:])
	crc.Write(metaBytes)
	crc.Write(payload)
	if checksum != crc.Sum32() {
		t.Fatalf("unexpected checksum: got %d want %d", checksum, crc.Sum32())
	}
}
