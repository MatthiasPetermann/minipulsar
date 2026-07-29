package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	pulsar "minipulsar/pb"
)

// handleFrame reads a length-prefixed Pulsar frame and dispatches the command.
// It enforces size limits to protect memory consumption.
func (b *Broker) handleFrame(conn net.Conn) error {
	if b.cfg.ReadTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(b.cfg.ReadTimeout))
	}
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

	var handlerErr error
	switch base.GetType() {
	case pulsar.BaseCommand_CONNECT:
		handlerErr = b.handleConnect(conn, &base)
	case pulsar.BaseCommand_PRODUCER:
		handlerErr = b.handleProducer(conn, &base)
	case pulsar.BaseCommand_SUBSCRIBE:
		handlerErr = b.handleSubscribe(conn, &base)
	case pulsar.BaseCommand_SEND:
		payloadSection, _ := io.ReadAll(r)
		handlerErr = b.handleSend(conn, &base, payloadSection)
	case pulsar.BaseCommand_FLOW:
		handlerErr = b.handleFlow(conn, &base)
	case pulsar.BaseCommand_ACK:
		handlerErr = b.handleAck(conn, &base)
	case pulsar.BaseCommand_SEEK:
		handlerErr = b.handleSeek(conn, &base)
	case pulsar.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		handlerErr = b.handleRedeliver(conn, &base)
	case pulsar.BaseCommand_GET_LAST_MESSAGE_ID:
		handlerErr = b.handleGetLastMessageID(conn, &base)
	case pulsar.BaseCommand_PING:
		handlerErr = b.handlePing(conn, &base)
	case pulsar.BaseCommand_PARTITIONED_METADATA:
		handlerErr = b.handlePartitionedMetadata(conn, &base)
	case pulsar.BaseCommand_LOOKUP:
		handlerErr = b.handleLookup(conn, &base)
	case pulsar.BaseCommand_CLOSE_PRODUCER:
		handlerErr = b.handleCloseProducer(conn, &base)
	case pulsar.BaseCommand_CLOSE_CONSUMER:
		handlerErr = b.handleCloseConsumer(conn, &base)
	default:
		handlerErr = fmt.Errorf("unsupported command type %s", base.GetType())
	}
	if handlerErr == nil {
		return nil
	}
	b.cfg.Logger.Warn("command rejected", "type", base.GetType(), "err", handlerErr)
	if send := base.GetSend(); send != nil {
		return b.writeCommand(conn, &pulsar.BaseCommand{
			Type: pulsar.BaseCommand_SEND_ERROR.Enum(),
			SendError: &pulsar.CommandSendError{
				ProducerId: proto.Uint64(send.GetProducerId()),
				SequenceId: proto.Uint64(send.GetSequenceId()),
				Error:      pulsar.ServerError_UnknownError.Enum(),
				Message:    proto.String(handlerErr.Error()),
			},
		})
	}
	return b.writeCommand(conn, commandError(&base, handlerErr))
}

func commandError(base *pulsar.BaseCommand, err error) *pulsar.BaseCommand {
	requestID := uint64(0)
	switch {
	case base.GetProducer() != nil:
		requestID = base.GetProducer().GetRequestId()
	case base.GetSubscribe() != nil:
		requestID = base.GetSubscribe().GetRequestId()
	case base.GetLookupTopic() != nil:
		requestID = base.GetLookupTopic().GetRequestId()
	case base.GetPartitionMetadata() != nil:
		requestID = base.GetPartitionMetadata().GetRequestId()
	case base.GetSeek() != nil:
		requestID = base.GetSeek().GetRequestId()
	case base.GetGetLastMessageId() != nil:
		requestID = base.GetGetLastMessageId().GetRequestId()
	case base.GetCloseProducer() != nil:
		requestID = base.GetCloseProducer().GetRequestId()
	case base.GetCloseConsumer() != nil:
		requestID = base.GetCloseConsumer().GetRequestId()
	}
	errorCode := pulsar.ServerError_UnknownError
	if strings.HasPrefix(err.Error(), "unsupported command") {
		errorCode = pulsar.ServerError_UnsupportedVersionError
	}
	return &pulsar.BaseCommand{
		Type: pulsar.BaseCommand_ERROR.Enum(),
		Error: &pulsar.CommandError{
			RequestId: proto.Uint64(requestID),
			Error:     errorCode.Enum(),
			Message:   proto.String(err.Error()),
		},
	}
}
