package broker

import (
	"io"
	"net"
	"sync"

	"minipulsar/internal/protocol"
	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

// Serve listens for TCP connections and starts a goroutine per connection.
// It blocks forever until the listener fails.
func (b *Broker) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.cfg.Logger.WithField("addr", addr).Info("minipulsar listening")

	for {
		conn, err := ln.Accept()
		if err != nil {
			b.cfg.Logger.WithError(err).Warn("accept error")
			continue
		}
		go b.handleConnection(conn)
	}
}

// handleConnection owns the lifecycle for a single TCP connection.
// It reads frames sequentially, dispatching each to the protocol handlers.
func (b *Broker) handleConnection(conn net.Conn) {
	defer func() {
		b.cleanupConnection(conn)
		_ = conn.Close()
		// optional: remove conn write mutex
		b.connWrite.Delete(conn)
	}()

	remote := conn.RemoteAddr().String()
	b.cfg.Logger.WithField("remote", remote).Info("new connection")

	for {
		if err := b.handleFrame(conn); err != nil {
			entry := b.cfg.Logger.WithField("remote", remote)
			if err == io.EOF {
				entry.Info("connection closed")
			} else {
				entry.WithError(err).Warn("connection error")
			}
			return
		}
	}
}

// cleanupConnection removes producers and consumers bound to a connection.
// It also drops pending messages for the associated consumers.
func (b *Broker) cleanupConnection(conn net.Conn) {
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

	b.mu.Lock()
	delete(b.connRoles, conn)
	b.mu.Unlock()

	for _, k := range consumerKeys {
		b.removeConsumer(k)
	}
}

// wmu returns the mutex used to serialize writes for a given connection.
// This prevents frame interleaving across goroutines.
func (b *Broker) wmu(conn net.Conn) *sync.Mutex {
	v, _ := b.connWrite.LoadOrStore(conn, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// writeCommand sends a simple Pulsar command frame to a client.
func (b *Broker) writeCommand(conn net.Conn, cmd *pulsar.BaseCommand) error {
	mu := b.wmu(conn)
	mu.Lock()
	defer mu.Unlock()
	return protocol.WriteSimpleCommand(conn, cmd)
}

// writeMsgFrame sends a message frame to a consumer with write serialization.
func (b *Broker) writeMsgFrame(conn net.Conn, consumerID uint64, msg storage.Message) error {
	mu := b.wmu(conn)
	mu.Lock()
	defer mu.Unlock()
	return protocol.WriteMessageFrame(conn, consumerID, msg)
}
