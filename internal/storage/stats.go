package storage

// TopicStat captures message and pending counts for a topic.
type TopicStat struct {
	Topic        string
	MessageCount int
	PendingCount int
}

// StatsSnapshot aggregates storage-backed broker stats.
type StatsSnapshot struct {
	Topics        int
	Subscriptions int
	Pending       int
	TopTopics     []TopicStat
}

// StatsSnapshot returns high-level storage stats plus top topics by backlog.
func (s *Store) StatsSnapshot(limit int) (StatsSnapshot, error) {
	if limit <= 0 {
		limit = 10
	}

	var topics int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM (
			SELECT topic FROM messages
			UNION
			SELECT topic FROM subscriptions
		)`,
	).Scan(&topics); err != nil {
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

	rows, err := s.db.Query(
		`SELECT t.topic,
			COALESCE(m.message_count, 0) AS message_count,
			COALESCE(p.pending_count, 0) AS pending_count
		 FROM (
			SELECT topic FROM messages
			UNION
			SELECT topic FROM subscriptions
		 ) t
		 LEFT JOIN (
			SELECT topic, COUNT(*) AS message_count FROM messages GROUP BY topic
		 ) m ON m.topic = t.topic
		 LEFT JOIN (
			SELECT topic, COUNT(*) AS pending_count FROM subscription_pending GROUP BY topic
		 ) p ON p.topic = t.topic
		 ORDER BY pending_count DESC, message_count DESC, t.topic
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
		Topics:        topics,
		Subscriptions: subs,
		Pending:       pending,
		TopTopics:     top,
	}, nil
}
