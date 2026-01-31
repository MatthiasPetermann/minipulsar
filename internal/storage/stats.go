package storage

// TopicStat captures message and pending counts for a topic.
type TopicStat struct {
	Topic        string
	MessageCount int
	PendingCount int
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

// StatsSnapshot returns high-level storage stats plus top topics by backlog.
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
