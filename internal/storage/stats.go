package storage

import "minipulsar/internal/topic"

// TopicStat captures message and pending counts for a topic.
type TopicStat struct {
	Topic        string
	MessageCount int
	PendingCount int
}

// SubscriptionBacklogStat captures undelivered backlog per subscription.
type SubscriptionBacklogStat struct {
	Topic        string
	Subscription string
	BacklogCount int
}

// StatsSnapshot aggregates storage-backed broker stats.
type StatsSnapshot struct {
	Namespaces    int
	Messages      int
	Topics        int
	Subscriptions int
	Pending       int
	TopTopics     []TopicStat
}

// StatsSnapshot returns high-level storage stats plus top topics by pending messages.
func (s *Store) StatsSnapshot(limit int) (StatsSnapshot, error) {
	if limit <= 0 {
		limit = 10
	}

	var topics int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM topics`,
	).Scan(&topics); err != nil {
		return StatsSnapshot{}, err
	}

	var namespaces int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM namespaces`,
	).Scan(&namespaces); err != nil {
		return StatsSnapshot{}, err
	}

	var subs int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM subscriptions").Scan(&subs); err != nil {
		return StatsSnapshot{}, err
	}

	var pending int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM subscription_pending").Scan(&pending); err != nil {
		return StatsSnapshot{}, err
	}

	var messages int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&messages); err != nil {
		return StatsSnapshot{}, err
	}

	rows, err := s.db.Query(
		`SELECT t.full_name,
			COALESCE(m.message_count, 0) AS message_count,
			COALESCE(p.pending_count, 0) AS pending_count
		 FROM topics t
		 LEFT JOIN (
			SELECT topic_id, COUNT(*) AS message_count FROM messages GROUP BY topic_id
		 ) m ON m.topic_id = t.id
		 LEFT JOIN (
			SELECT topic_id, COUNT(*) AS pending_count FROM subscription_pending GROUP BY topic_id
		 ) p ON p.topic_id = t.id
		 ORDER BY pending_count DESC, message_count DESC, t.full_name
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return StatsSnapshot{}, err
	}
	defer rows.Close()

	var top []TopicStat
	for rows.Next() {
		var stat TopicStat
		if err := rows.Scan(&stat.Topic, &stat.MessageCount, &stat.PendingCount); err != nil {
			return StatsSnapshot{}, err
		}
		top = append(top, stat)
	}
	if err := rows.Err(); err != nil {
		return StatsSnapshot{}, err
	}

	return StatsSnapshot{
		Namespaces:    namespaces,
		Messages:      messages,
		Topics:        topics,
		Subscriptions: subs,
		Pending:       pending,
		TopTopics:     top,
	}, nil
}

// SubscriptionBacklogStats returns undelivered backlog counts per subscription.
func (s *Store) SubscriptionBacklogStats(namespace string, limit int) ([]SubscriptionBacklogStat, error) {
	info, err := topic.Parse(namespace + "/__validate")
	if err != nil {
		return nil, err
	}
	if !info.Persistent {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.Query(
		`SELECT t.full_name,
			s.name,
			COALESCE(p.pending_count, 0) + COALESCE(SUM(CASE WHEN sp.message_id IS NULL THEN 1 ELSE 0 END), 0) AS backlog_count
		 FROM subscriptions s
		 JOIN topics t ON t.id = s.topic_id
		 JOIN namespaces n ON n.id = t.namespace_id
		 JOIN subscription_cursor c ON c.topic_id = s.topic_id AND c.name = s.name
		 LEFT JOIN (
			SELECT topic_id, name, COUNT(*) AS pending_count
			FROM subscription_pending
			GROUP BY topic_id, name
		 ) p ON p.topic_id = s.topic_id AND p.name = s.name
		 LEFT JOIN messages m ON m.topic_id = t.id AND m.id >= c.next_message_id
		 LEFT JOIN subscription_pending sp
			ON sp.topic_id = s.topic_id
		   AND sp.name = s.name
		   AND sp.message_id = m.id
		 WHERE n.tenant = ? AND n.name = ?
		 GROUP BY t.full_name, s.name, p.pending_count
		 ORDER BY backlog_count DESC, t.full_name, s.name
		 LIMIT ?`,
		info.Tenant,
		info.Namespace,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []SubscriptionBacklogStat
	for rows.Next() {
		var stat SubscriptionBacklogStat
		if err := rows.Scan(&stat.Topic, &stat.Subscription, &stat.BacklogCount); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stats, nil
}
