// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

// Package outbox implements the transactional outbox pattern on top of
// PostgreSQL: domain messages are persisted in the same transaction as the
// business data and later relayed to a broker by a polling publisher.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"time"
)

// Consumption status flags stored in the outbox table.
const (
	statusConsumed    = true
	statusNotConsumed = false
)

// maxBatchSize caps how many rows a single relay iteration may pull.
const maxBatchSize = 10000

// ErrBatchSizeOutOfRange is returned when a relay is started with a batch size
// outside the (0, maxBatchSize] range.
var ErrBatchSizeOutOfRange = errors.New("batch size out of range")

// ErrUnsupportedPayload is returned when a message payload cannot be converted
// to raw bytes for publishing.
var ErrUnsupportedPayload = errors.New("unsupported payload type")

// Tunables shared across the package. They are package-level so callers can
// override defaults once during bootstrap without threading config everywhere.
var (
	// TableName is the outbox table the relay reads from and persisters write to.
	TableName = "outbox_messages"

	// PublishRetryDelay is the pause between publish retries.
	PublishRetryDelay = time.Second

	// PublishRetryAttempts is how many times a single message is retried before
	// it is left for the next relay iteration.
	PublishRetryAttempts uint = 3

	// PartitionKeyAlgorithm maps a partition string to a numeric key used to keep
	// related messages on the same relay worker.
	PartitionKeyAlgorithm PartitionKeyAlg = partitionKey
)

// BatchSize is the number of messages fetched per relay iteration.
type BatchSize uint

// Valid reports whether the batch size is within the allowed range.
func (b BatchSize) Valid() error {
	if b == 0 || b > maxBatchSize {
		return ErrBatchSizeOutOfRange
	}

	return nil
}

// PartitionKeyAlg derives a numeric partition key from a string.
type PartitionKeyAlg func(s string) int

// Publisher delivers a message to the broker under the given exchange and topic.
type Publisher interface {
	Publish(exchange, topic string, message Message) error
}

// EventRepository reads pending messages and marks them as consumed.
type EventRepository interface {
	Fetch(ctx context.Context, batchSize BatchSize) <-chan Message
	MarkConsumed(ctx context.Context, msgs []Message) error
}

// Message is a single outbox record. Headers carry transport metadata
// (trace ids, content type, custom keys) and are persisted alongside the
// payload in the headers column.
type Message struct {
	ID           string
	EventType    string
	Payload      any
	Headers      map[string]any
	PartitionKey sql.NullInt64
	Exchange     string
	RoutingKey   string
	Consumed     bool
	CreatedAt    time.Time
}

// NewMessage builds a message ready to be persisted. partition is hashed via
// PartitionKeyAlgorithm so related messages share a relay worker. Headers are
// optional and can be set on the returned message via the Headers field.
func NewMessage(id, eventType string, payload any, exchange, partition, routingKey string) Message {
	return Message{
		ID:         id,
		EventType:  eventType,
		Payload:    payload,
		Exchange:   exchange,
		RoutingKey: routingKey,
		CreatedAt:  time.Now(),
		PartitionKey: sql.NullInt64{
			Int64: int64(PartitionKeyAlgorithm(partition)),
			Valid: true,
		},
	}
}

// BytePayload returns the payload as raw bytes. Only string and []byte payloads
// are supported; anything else must be serialized by the caller beforehand.
func (m *Message) BytePayload() ([]byte, error) {
	switch p := m.Payload.(type) {
	case string:
		return []byte(p), nil
	case []byte:
		return p, nil
	default:
		return nil, ErrUnsupportedPayload
	}
}

// partitionKey is the default FNV-1a based partition algorithm.
func partitionKey(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))

	return int(h.Sum32())
}
