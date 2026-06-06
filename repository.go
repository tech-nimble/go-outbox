// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"fmt"
)

// markConsumedBatch is the maximum number of message ids sent in a single
// MarkConsumed UPDATE statement.
const markConsumedBatch = 1000

// Repository reads and updates outbox messages through a DBAdapter.
type Repository struct {
	db DBAdapter
}

// NewRepository builds a repository over the given adapter.
func NewRepository(db DBAdapter) *Repository {
	return &Repository{db: db}
}

// Fetch streams unconsumed messages, oldest first, up to batchSize. The channel
// is closed once the result set is drained or the context is canceled.
func (r *Repository) Fetch(ctx context.Context, batchSize BatchSize) <-chan Message {
	stream := make(chan Message, batchSize)

	query := fmt.Sprintf(
		"SELECT event_id, event_type, exchange, routing_key, partition_key, payload, headers, consumed, created_at "+
			"FROM %s WHERE consumed = $1 ORDER BY created_at ASC LIMIT $2",
		TableName,
	)

	go func() {
		defer close(stream)

		rows, err := r.db.Query(ctx, query, statusNotConsumed, batchSize)
		if err != nil {
			logger.Error().Err(err).Msg("while querying messages")

			return
		}

		defer func() {
			if err := rows.Close(); err != nil {
				logger.Error().Err(err).Msg("while closing rows")
			}
		}()

		for rows.Next() {
			var (
				msg        Message
				headersRaw any
			)

			if err := rows.Scan(
				&msg.ID, &msg.EventType, &msg.Exchange, &msg.RoutingKey,
				&msg.PartitionKey, &msg.Payload, &headersRaw, &msg.Consumed, &msg.CreatedAt,
			); err != nil {
				logger.Error().Err(err).Msg("while scanning message")

				continue
			}

			msg.Headers, err = decodeHeaders(headersRaw)
			if err != nil {
				logger.Error().Err(err).Str("message_id", msg.ID).Msg("while decoding headers")

				continue
			}

			select {
			case stream <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return stream
}

// MarkConsumed flags the given messages as consumed in chunks so the IN clause
// stays bounded.
func (r *Repository) MarkConsumed(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s SET consumed = ? WHERE event_id IN ?", TableName)

	ids := make([]string, 0, markConsumedBatch)
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}

		if err := r.db.Exec(ctx, query, statusConsumed, ids); err != nil {
			return fmt.Errorf("%w: update consumed status", err)
		}

		ids = ids[:0]

		return nil
	}

	for i := range msgs {
		if msgs[i].ID == "" {
			continue
		}

		ids = append(ids, msgs[i].ID)

		if len(ids) == markConsumedBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}
