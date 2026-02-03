package broker

import (
	"time"

	"minipulsar/internal/storage"
	"minipulsar/internal/topic"
)

func (b *Broker) publishMessage(info topic.Info, msg storage.Message) (storage.Message, error) {
	if msg.PublishTime == 0 {
		msg.PublishTime = time.Now().UnixMilli()
	}
	msg.Topic = info.FullName

	if info.Persistent {
		if b.shouldPersistMessage(info) {
			if err := b.store.InsertMessage(&msg); err != nil {
				return msg, err
			}
			b.kickTopic(info.FullName)
		} else {
			msg.ID = b.nextNonPersistentID(info.FullName)
		}
		return msg, nil
	}

	msg.ID = b.nextNonPersistentID(info.FullName)
	b.deliverNonPersistent(info.FullName, msg)
	return msg, nil
}
