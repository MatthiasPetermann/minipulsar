package broker

import (
	"time"

	"minipulsar/internal/storage"
	"minipulsar/internal/topic"
)

// publishMessage persists every persistent message and directly delivers
// non-persistent messages. Retention controls later cleanup, never publication.
func (b *Broker) publishMessage(info topic.Info, msg storage.Message) (storage.Message, error) {
	if msg.PublishTime == 0 {
		msg.PublishTime = time.Now().UnixMilli()
	}
	msg.Topic = info.FullName

	if info.Persistent {
		if err := b.store.InsertMessage(&msg); err != nil {
			return msg, err
		}
		b.kickTopic(info.FullName)
		return msg, nil
	}

	msg.ID = b.nextNonPersistentID(info.FullName)
	b.deliverNonPersistent(info.FullName, msg)
	return msg, nil
}
