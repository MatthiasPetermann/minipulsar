package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

const (
	// MagicMessageFormat marks the start of a message metadata+payload frame.
	// Pulsar uses this to distinguish command frames from message frames.
	MagicMessageFormat uint16 = 0x0e01
)

// WriteSimpleCommand writes a length-prefixed Pulsar command frame.
// It is used for control-plane messages like CONNECT, SUBSCRIBE, or ACK.
func WriteSimpleCommand(w io.Writer, cmd *pulsar.BaseCommand) error {
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

// WriteMessageFrame writes a Pulsar message frame containing command + metadata + payload.
// The storage message ID becomes the Pulsar entry ID when sending to consumers.
func WriteMessageFrame(w io.Writer, consumerID uint64, msg storage.Message) error {
	base := &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_MESSAGE.Enum(),
		Message: &pulsar.CommandMessage{
			ConsumerId: proto.Uint64(consumerID),
			MessageId: &pulsar.MessageIdData{
				LedgerId: proto.Uint64(0),
				EntryId:  proto.Uint64(uint64(msg.ID)),
			},
		},
	}
	cmdBytes, err := proto.Marshal(base)
	if err != nil {
		return fmt.Errorf("marshal base message: %w", err)
	}

	publishTime := msg.PublishTime
	if publishTime == 0 {
		publishTime = time.Now().UnixMilli()
	}

	meta := &pulsar.MessageMetadata{
		ProducerName: proto.String("minipulsar"),
		SequenceId:   proto.Uint64(msg.SequenceID),
		PublishTime:  proto.Uint64(uint64(publishTime)),
		Properties:   KeyValuesFromProperties(msg.Properties),
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
	crc.Write(msg.Payload)
	checksum := crc.Sum32()

	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, uint32(len(cmdBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(cmdBytes); err != nil {
		return err
	}

	if err := binary.Write(&buf, binary.BigEndian, MagicMessageFormat); err != nil {
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
	if _, err := buf.Write(msg.Payload); err != nil {
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
