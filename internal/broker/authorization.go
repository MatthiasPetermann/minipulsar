package broker

import (
	"fmt"
	"net"

	"minipulsar/internal/messaging"
	"minipulsar/internal/topic"
)

func (b *Broker) namespaceFromTopic(info topic.Info) string {
	scheme := "persistent"
	if !info.Persistent {
		scheme = "non-persistent"
	}
	return fmt.Sprintf("%s://%s/%s", scheme, info.Tenant, info.Namespace)
}

func (b *Broker) rolesForConn(conn net.Conn) []string {
	b.mu.RLock()
	roles := b.connRoles[conn]
	b.mu.RUnlock()
	return roles
}

func (b *Broker) authorize(conn net.Conn, info topic.Info, action messaging.Action) error {
	if b.cfg.Messaging == nil || b.cfg.Messaging.Security == nil {
		return nil
	}
	namespace := b.namespaceFromTopic(info)
	roles := b.rolesForConn(conn)
	if b.cfg.Messaging.Security.Allows(namespace, action, roles) {
		return nil
	}
	return fmt.Errorf("unauthorized %s on namespace %s", action, namespace)
}
