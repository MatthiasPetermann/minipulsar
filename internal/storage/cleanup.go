package storage

import (
	"database/sql"
	"time"

	"minipulsar/internal/topic"
)

// DroppedSubscription identifies a subscription removed by cleanup.
type DroppedSubscription struct {
	Topic        string
	Subscription string
}

// PruneStaleSubscriptions removes subscriptions that have not been served recently.
func (s *Store) PruneStaleSubscriptions(namespace string, cutoff time.Time) ([]DroppedSubscription, error) {
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return nil, err
	}
	if !info.Persistent {
		return nil, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(
		`SELECT s.topic_id, s.name, t.full_name
		 FROM subscriptions s
		 JOIN topics t ON t.id = s.topic_id
		 JOIN namespaces n ON n.id = t.namespace_id
		 WHERE n.tenant = ? AND n.name = ? AND s.last_consumer_at > 0 AND s.last_consumer_at < ?`,
		info.Tenant, info.Namespace, cutoff.UnixMilli(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type subKey struct {
		topicID int64
		name    string
		topic   string
	}
	var stale []subKey
	for rows.Next() {
		var entry subKey
		if err := rows.Scan(&entry.topicID, &entry.name, &entry.topic); err != nil {
			return nil, err
		}
		stale = append(stale, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var dropped []DroppedSubscription
	for _, entry := range stale {
		if _, err := tx.Exec(
			"DELETE FROM subscription_pending WHERE topic_id=? AND name=?",
			entry.topicID, entry.name,
		); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(
			"DELETE FROM subscription_cursor WHERE topic_id=? AND name=?",
			entry.topicID, entry.name,
		); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(
			"DELETE FROM subscriptions WHERE topic_id=? AND name=?",
			entry.topicID, entry.name,
		); err != nil {
			return nil, err
		}
		dropped = append(dropped, DroppedSubscription{Topic: entry.topic, Subscription: entry.name})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return dropped, nil
}

// PruneOrphanedMessages removes messages when no subscriptions exist for a topic.
func (s *Store) PruneOrphanedMessages(namespace string, cutoff time.Time) (int64, error) {
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return 0, err
	}
	if !info.Persistent {
		return 0, nil
	}
	res, err := s.db.Exec(
		`DELETE FROM messages
		 WHERE topic_id IN (
			SELECT t.id
			  FROM topics t
			  JOIN namespaces n ON n.id = t.namespace_id
			 WHERE n.tenant = ? AND n.name = ?
			   AND NOT EXISTS (
				 SELECT 1 FROM subscriptions s WHERE s.topic_id = t.id
			   )
		  )
		  AND publish_time < ?`,
		info.Tenant, info.Namespace, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func scanSubscriptionCursor(db *sql.DB, sub string) (int64, error) {
	var nextID int64
	err := db.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE name=?",
		sub,
	).Scan(&nextID)
	return nextID, err
}
