package storage

import (
	"database/sql"
	"fmt"
	"time"

	"minipulsar/internal/topic"
)

// Message represents a persisted Pulsar message in the broker storage layer.
// It carries the minimum metadata needed to deliver and acknowledge messages
// while keeping the wire-level framing logic elsewhere.
type Message struct {
	ID          int64
	Topic       string
	Payload     []byte
	SequenceID  uint64
	PublishTime int64
}

// Store wraps the SQLite database connection used for durability.
// It encapsulates all SQL access so the broker can remain storage-agnostic.
type Store struct {
	db *sql.DB
}

// Open creates a new Store backed by the provided SQLite database path.
// The caller must call InitSchema before serving traffic.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// DB exposes the underlying database handle for advanced callers.
// Most code should remain within the Store API to keep concerns isolated.
func (s *Store) DB() *sql.DB {
	return s.db
}

// InitSchema creates the tables required for Pulsar topics, subscriptions,
// delivery cursors, and pending acknowledgements.
func (s *Store) InitSchema() error {
	schema := `
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS namespaces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant TEXT NOT NULL,
  name TEXT NOT NULL,
  UNIQUE (tenant, name)
);

CREATE TABLE IF NOT EXISTS topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  full_name TEXT NOT NULL UNIQUE,
  FOREIGN KEY (namespace_id) REFERENCES namespaces(id)
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic_id INTEGER NOT NULL,
  payload BLOB NOT NULL,
  publish_time INTEGER NOT NULL,
  sequence_id INTEGER NOT NULL,
  FOREIGN KEY (topic_id) REFERENCES topics(id)
);

CREATE TABLE IF NOT EXISTS subscriptions (
  topic_id INTEGER NOT NULL,
  name  TEXT NOT NULL,
  type  TEXT NOT NULL DEFAULT 'shared',
  PRIMARY KEY (topic_id, name),
  FOREIGN KEY (topic_id) REFERENCES topics(id)
);

-- Dispatch cursor: next message id to CLAIM/DISPATCH.
-- IMPORTANT: This is NOT derived from ack; it moves only when claiming.
CREATE TABLE IF NOT EXISTS subscription_cursor (
  topic_id INTEGER NOT NULL,
  name  TEXT NOT NULL,
  next_message_id INTEGER NOT NULL,
  PRIMARY KEY (topic_id, name),
  FOREIGN KEY (topic_id, name) REFERENCES subscriptions(topic_id, name)
);

-- Pending set: messages claimed (delivered) but not yet acked.
CREATE TABLE IF NOT EXISTS subscription_pending (
  topic_id INTEGER NOT NULL,
  name  TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  consumer_id INTEGER NOT NULL,
  delivered_at INTEGER NOT NULL,
  PRIMARY KEY (topic_id, name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_by_sub
  ON subscription_pending(topic_id, name, message_id);

CREATE INDEX IF NOT EXISTS idx_messages_by_topic
  ON messages(topic_id, id);

CREATE INDEX IF NOT EXISTS idx_topics_by_namespace
  ON topics(namespace_id, name);
`
	_, err := s.db.Exec(schema)
	return err
}

// EnsureSubscription ensures the subscription exists and has an initialized cursor.
// This matches Pulsar's semantics where subscriptions are created on demand.
func (s *Store) EnsureSubscription(topicName, name string) error {
	info, err := topic.Parse(topicName)
	if err != nil {
		return err
	}
	if !info.Persistent {
		return fmt.Errorf("non-persistent topic cannot be stored: %s", topicName)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	topicID, err := ensureTopic(tx, info)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscriptions(topic_id, name, type) VALUES(?, ?, 'shared')",
		topicID, name,
	); err != nil {
		return err
	}

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscription_cursor(topic_id, name, next_message_id) VALUES(?, ?, 1)",
		topicID, name,
	); err != nil {
		return err
	}

	return tx.Commit()
}

// InsertMessage persists a message for a topic and sets the generated ID.
// The broker uses the ID as the Pulsar entry ID when sending messages.
func (s *Store) InsertMessage(msg *Message) error {
	info, err := topic.Parse(msg.Topic)
	if err != nil {
		return err
	}
	if !info.Persistent {
		return fmt.Errorf("non-persistent topic cannot be stored: %s", msg.Topic)
	}

	if msg.PublishTime == 0 {
		msg.PublishTime = time.Now().UnixMilli()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	topicID, err := ensureTopic(tx, info)
	if err != nil {
		return err
	}

	res, err := tx.Exec(
		"INSERT INTO messages(topic_id, payload, publish_time, sequence_id) VALUES(?, ?, ?, ?)",
		topicID, msg.Payload, msg.PublishTime, msg.SequenceID,
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	msg.ID = id
	return tx.Commit()
}

// AckIndividual clears pending entries for the given consumer and message IDs.
// It models Pulsar's individual acknowledgment behavior for shared subscriptions.
func (s *Store) AckIndividual(topicName, sub string, consumerUID int64, ids []int64) error {
	topicID, err := s.lookupTopicID(topicName)
	if err != nil {
		return err
	}
	if topicID == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, msgID := range ids {
		if _, err := tx.Exec(
			"DELETE FROM subscription_pending WHERE topic_id=? AND name=? AND message_id=? AND consumer_id=?",
			topicID, sub, msgID, consumerUID,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DropPendingByConsumer removes all pending entries for a consumer.
// This is invoked when connections close so other consumers can progress.
func (s *Store) DropPendingByConsumer(topicName, sub string, consumerUID int64) error {
	topicID, err := s.lookupTopicID(topicName)
	if err != nil {
		return err
	}
	if topicID == 0 {
		return nil
	}

	_, err = s.db.Exec(
		"DELETE FROM subscription_pending WHERE topic_id=? AND name=? AND consumer_id=?",
		topicID, sub, consumerUID,
	)
	return err
}

// ClaimBatch atomically claims a batch of messages for delivery.
// It advances the cursor and inserts pending rows so other consumers
// do not see the same messages until they are acknowledged.
func (s *Store) ClaimBatch(topicName, sub string, consumerUID int64, limit int) ([]Message, error) {
	info, err := topic.Parse(topicName)
	if err != nil {
		return nil, err
	}
	if !info.Persistent {
		return nil, fmt.Errorf("non-persistent topic cannot be stored: %s", topicName)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	topicID, err := ensureTopic(tx, info)
	if err != nil {
		return nil, err
	}

	// Ensure cursor row exists.
	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscription_cursor(topic_id, name, next_message_id) VALUES(?, ?, 1)",
		topicID, sub,
	); err != nil {
		return nil, err
	}

	// Read cursor.
	var cur int64
	if err := tx.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE topic_id=? AND name=?",
		topicID, sub,
	).Scan(&cur); err != nil {
		return nil, err
	}

	// Select deliverable messages (not already pending).
	rows, err := tx.Query(
		`SELECT m.id, t.full_name, m.payload, m.publish_time, m.sequence_id
		 FROM messages m
		 JOIN topics t ON t.id = m.topic_id
		 WHERE m.topic_id = ?
		   AND m.id >= ?
		   AND NOT EXISTS (
			 SELECT 1 FROM subscription_pending p
			 WHERE p.topic_id = ? AND p.name = ? AND p.message_id = m.id
		   )
		 ORDER BY m.id
		 LIMIT ?`,
		topicID, cur, topicID, sub, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Message
	var lastID int64
	now := time.Now().UnixMilli()

	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.PublishTime, &m.SequenceID); err != nil {
			return nil, err
		}

		// Insert pending (guarantees no other consumer can claim it later).
		if _, err := tx.Exec(
			"INSERT OR IGNORE INTO subscription_pending(topic_id, name, message_id, consumer_id, delivered_at) VALUES(?,?,?,?,?)",
			topicID, sub, m.ID, consumerUID, now,
		); err != nil {
			return nil, err
		}

		res = append(res, m)
		lastID = m.ID
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Advance cursor monotonically to lastID+1 (dispatch cursor).
	if len(res) > 0 {
		if _, err := tx.Exec(
			"UPDATE subscription_cursor SET next_message_id=? WHERE topic_id=? AND name=?",
			lastID+1, topicID, sub,
		); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Store) lookupTopicID(topicName string) (int64, error) {
	info, err := topic.Parse(topicName)
	if err != nil {
		return 0, err
	}
	if !info.Persistent {
		return 0, nil
	}

	var id int64
	err = s.db.QueryRow(
		`SELECT t.id
		 FROM topics t
		 JOIN namespaces n ON n.id = t.namespace_id
		 WHERE n.tenant = ? AND n.name = ? AND t.name = ?`,
		info.Tenant, info.Namespace, info.Name,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

func ensureTopic(tx *sql.Tx, info topic.Info) (int64, error) {
	if !info.Persistent {
		return 0, fmt.Errorf("non-persistent topic cannot be stored: %s", info.FullName)
	}

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO namespaces(tenant, name) VALUES(?, ?)",
		info.Tenant, info.Namespace,
	); err != nil {
		return 0, err
	}

	var namespaceID int64
	if err := tx.QueryRow(
		"SELECT id FROM namespaces WHERE tenant=? AND name=?",
		info.Tenant, info.Namespace,
	).Scan(&namespaceID); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO topics(namespace_id, name, full_name) VALUES(?, ?, ?)",
		namespaceID, info.Name, info.FullName,
	); err != nil {
		return 0, err
	}

	var topicID int64
	if err := tx.QueryRow(
		"SELECT id FROM topics WHERE namespace_id=? AND name=?",
		namespaceID, info.Name,
	).Scan(&topicID); err != nil {
		return 0, err
	}

	return topicID, nil
}
