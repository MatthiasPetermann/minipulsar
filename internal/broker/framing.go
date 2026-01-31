package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"

	pulsar "minipulsar/pb"
)

// handleFrame reads a length-prefixed Pulsar frame and dispatches the command.
// It enforces size limits to protect memory consumption.
func (b *Broker) handleFrame(conn net.Conn) error {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return err
	}
	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	if totalSize == 0 {
		return fmt.Errorf("invalid frame size 0")
	}
	if totalSize > b.cfg.MaxFrameSize {
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
		b.cfg.Logger.Warn("unhandled command type", "type", base.GetType())
		return nil
	}
}
