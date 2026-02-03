package storage

import (
	"fmt"
	"time"

	"minipulsar/internal/topic"
)

// SubscriptionRef identifies a subscription by topic and name.
type SubscriptionRef struct {
	Topic        string
	Subscription string
}

// TouchSubscriptions updates last_consumer_at for the provided subscriptions.
func (s *Store) TouchSubscriptions(namespace string, subs []SubscriptionRef) error {
	if len(subs) == 0 {
		return nil
	}
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return err
	}
	if !info.Persistent {
		return nil
	}
	now := time.Now().UnixMilli()
	for _, sub := range subs {
		if sub.Topic == "" || sub.Subscription == "" {
			continue
		}
		topicInfo, err := topic.Parse(sub.Topic)
		if err != nil {
			return err
		}
		if topicInfo.FullName != sub.Topic {
			return fmt.Errorf("unexpected topic format: %s", sub.Topic)
		}
		if topicInfo.Tenant != info.Tenant || topicInfo.Namespace != info.Namespace || topicInfo.Persistent != info.Persistent {
			continue
		}
		if _, err := s.db.Exec(
			`UPDATE subscriptions
			 SET last_consumer_at=?
			 WHERE topic_id=(SELECT id FROM topics WHERE full_name=?)
			   AND name=?`,
			now,
			sub.Topic,
			sub.Subscription,
		); err != nil {
			return err
		}
	}
	return nil
}
