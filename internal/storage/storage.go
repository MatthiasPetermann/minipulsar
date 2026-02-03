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

// SubscriptionInitialPosition determines where the subscription cursor starts.
type SubscriptionInitialPosition int

const (
	InitialPositionLatest SubscriptionInitialPosition = iota
	InitialPositionEarliest
)

// SubscriptionType describes how a subscription delivers messages to consumers.
type SubscriptionType string

const (
	SubscriptionTypeExclusive SubscriptionType = "exclusive"
	SubscriptionTypeShared    SubscriptionType = "shared"
	SubscriptionTypeFailover  SubscriptionType = "failover"
)

// Open creates a new Store backed by the provided SQLite database path.
// The caller must call InitSchema before serving traffic.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
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
PRAGMA busy_timeout=5000;

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
  created_at INTEGER NOT NULL,
  last_consumer_at INTEGER NOT NULL,
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
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}
	if err := addColumnIfMissing(s.db, "subscriptions", "created_at", "ALTER TABLE subscriptions ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := addColumnIfMissing(s.db, "subscriptions", "last_consumer_at", "ALTER TABLE subscriptions ADD COLUMN last_consumer_at INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	return nil
}

// EnsureSubscription ensures the subscription exists and has an initialized cursor.
// This matches Pulsar's semantics where subscriptions are created on demand.
func (s *Store) EnsureSubscription(topicName, name string, position SubscriptionInitialPosition, subType SubscriptionType) error {
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

	now := time.Now().UnixMilli()
	var existingType string
	err = tx.QueryRow(
		"SELECT type FROM subscriptions WHERE topic_id=? AND name=?",
		topicID, name,
	).Scan(&existingType)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == sql.ErrNoRows {
		if subType == "" {
			subType = SubscriptionTypeShared
		}
		nextID := int64(1)
		if position == InitialPositionLatest {
			var maxID int64
			if err := tx.QueryRow(
				"SELECT COALESCE(MAX(id), 0) FROM messages WHERE topic_id=?",
				topicID,
			).Scan(&maxID); err != nil {
				return err
			}
			nextID = maxID + 1
		}
		if _, err := tx.Exec(
			"INSERT INTO subscriptions(topic_id, name, type, created_at, last_consumer_at) VALUES(?, ?, ?, ?, ?)",
			topicID, name, string(subType), now, now,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT OR REPLACE INTO subscription_cursor(topic_id, name, next_message_id) VALUES(?, ?, ?)",
			topicID, name, nextID,
		); err != nil {
			return err
		}
	} else {
		if subType != "" && existingType != string(subType) {
			return fmt.Errorf("subscription %s type mismatch: existing %s requested %s", name, existingType, subType)
		}
		if _, err := tx.Exec(
			"UPDATE subscriptions SET last_consumer_at=? WHERE topic_id=? AND name=?",
			now, topicID, name,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"INSERT OR IGNORE INTO subscription_cursor(topic_id, name, next_message_id) VALUES(?, ?, 1)",
			topicID, name,
		); err != nil {
			return err
		}
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

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(
		"INSERT OR IGNORE INTO subscription_cursor(topic_id, name, next_message_id) VALUES(?, ?, 1)",
		topicID, sub,
	); err != nil {
		return err
	}

	var minPending int64
	if err := tx.QueryRow(
		"SELECT COALESCE(MIN(message_id), 0) FROM subscription_pending WHERE topic_id=? AND name=? AND consumer_id=?",
		topicID, sub, consumerUID,
	).Scan(&minPending); err != nil {
		return err
	}

	var cur int64
	if err := tx.QueryRow(
		"SELECT next_message_id FROM subscription_cursor WHERE topic_id=? AND name=?",
		topicID, sub,
	).Scan(&cur); err != nil {
		return err
	}

	if _, err := tx.Exec(
		"DELETE FROM subscription_pending WHERE topic_id=? AND name=? AND consumer_id=?",
		topicID, sub, consumerUID,
	); err != nil {
		return err
	}

	if minPending > 0 && (cur == 0 || minPending < cur) {
		if _, err := tx.Exec(
			"UPDATE subscription_cursor SET next_message_id=? WHERE topic_id=? AND name=?",
			minPending, topicID, sub,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
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

	if _, err := tx.Exec(
		"UPDATE subscriptions SET last_consumer_at=? WHERE topic_id=? AND name=?",
		time.Now().UnixMilli(), topicID, sub,
	); err != nil {
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
	var lastSeenID int64
	now := time.Now().UnixMilli()

	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.PublishTime, &m.SequenceID); err != nil {
			return nil, err
		}
		lastSeenID = m.ID

		// Insert pending (guarantees no other consumer can claim it later).
		result, err := tx.Exec(
			"INSERT OR IGNORE INTO subscription_pending(topic_id, name, message_id, consumer_id, delivered_at) VALUES(?,?,?,?,?)",
			topicID, sub, m.ID, consumerUID, now,
		)
		if err != nil {
			return nil, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			continue
		}

		res = append(res, m)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Advance cursor monotonically to last seen message ID + 1 (dispatch cursor).
	if lastSeenID > 0 {
		if _, err := tx.Exec(
			"UPDATE subscription_cursor SET next_message_id=? WHERE topic_id=? AND name=?",
			lastSeenID+1, topicID, sub,
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

// HasSubscriptions reports whether a topic has at least one subscription.
func (s *Store) HasSubscriptions(topicName string) (bool, error) {
	topicID, err := s.lookupTopicID(topicName)
	if err != nil {
		return false, err
	}
	if topicID == 0 {
		return false, nil
	}
	var exists int
	err = s.db.QueryRow(
		"SELECT 1 FROM subscriptions WHERE topic_id=? LIMIT 1",
		topicID,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

func addColumnIfMissing(db *sql.DB, table, column, ddl string) error {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(ddl)
	return err
}
