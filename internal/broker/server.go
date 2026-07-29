package broker

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"minipulsar/internal/protocol"
	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

// Serve listens for TCP connections and starts a goroutine per connection.
// It blocks forever until the listener fails.
func (b *Broker) Serve(addr string) error {
	return b.ServeWithTLS(addr, b.cfg.TLSConfig)
}

// ServeWithTLS listens for TCP connections with an optional TLS config.
// It blocks forever until the listener fails.
func (b *Broker) ServeWithTLS(addr string, tlsConfig *tls.Config) error {
	var ln net.Listener
	var err error
	if tlsConfig != nil {
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	return b.serveListener(ln, tlsConfig != nil)
}

func (b *Broker) serveListener(ln net.Listener, tlsEnabled bool) error {
	b.mu.Lock()
	b.listeners[ln] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.listeners, ln)
		b.mu.Unlock()
		_ = ln.Close()
	}()

	b.cfg.Logger.Info("minipulsar listening", "addr", ln.Addr(), "tls", tlsEnabled)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if b.lifecycleCtx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			b.cfg.Logger.Warn("accept error", "err", err)
			continue
		}
		if b.lifecycleCtx.Err() != nil {
			_ = conn.Close()
			return nil
		}
		b.mu.Lock()
		if b.lifecycleCtx.Err() != nil {
			b.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		if b.cfg.MaxConnections > 0 && len(b.connections) >= b.cfg.MaxConnections {
			b.mu.Unlock()
			b.cfg.Logger.Warn("connection limit reached", "limit", b.cfg.MaxConnections, "remote", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}
		b.connections[conn] = struct{}{}
		b.mu.Unlock()
		b.lifecycleWG.Add(1)
		go func() {
			defer b.lifecycleWG.Done()
			b.handleConnection(conn)
		}()
	}
}

// Shutdown stops listeners, closes active connections, and waits for broker work to finish.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.shutdownOnce.Do(func() {
		b.lifecycleCancel()
		b.mu.RLock()
		listeners := make([]net.Listener, 0, len(b.listeners))
		for listener := range b.listeners {
			listeners = append(listeners, listener)
		}
		connections := make([]net.Conn, 0, len(b.connections))
		for conn := range b.connections {
			connections = append(connections, conn)
		}
		b.mu.RUnlock()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		for _, conn := range connections {
			_ = conn.Close()
		}
	})

	done := make(chan struct{})
	go func() {
		b.lifecycleWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
	b.cfg.Logger.Info("new connection", "remote", remote)

	for {
		if err := b.handleFrame(conn); err != nil {
			entry := b.cfg.Logger.With("remote", remote)
			if err == io.EOF {
				entry.Info("connection closed")
			} else {
				entry.Warn("connection error", "err", err)
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
		p := b.producers[k]
		delete(b.producers, k)
		if p != nil {
			b.signalProducerWaitersLocked(p.topic)
		}
		b.mu.Unlock()
	}

	b.mu.Lock()
	delete(b.connRoles, conn)
	delete(b.connections, conn)
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
	b.setWriteDeadline(conn)
	return protocol.WriteSimpleCommand(conn, cmd)
}

// writeMsgFrame sends a message frame to a consumer with write serialization.
func (b *Broker) writeMsgFrame(conn net.Conn, consumerID uint64, msg storage.Message) error {
	mu := b.wmu(conn)
	mu.Lock()
	defer mu.Unlock()
	b.setWriteDeadline(conn)
	return protocol.WriteMessageFrame(conn, consumerID, msg)
}

// setWriteDeadline applies the configured write timeout if enabled.
func (b *Broker) setWriteDeadline(conn net.Conn) {
	if b.cfg.WriteTimeout <= 0 {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(b.cfg.WriteTimeout))
}
