// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

// insertColumns is the column list shared by both persisters when writing
// messages into the outbox table.
const insertColumns = "event_id, event_type, payload, headers, exchange, routing_key, partition_key"

// PgxPersister persists outbox messages within a pgx transaction.
type PgxPersister struct {
	pool *pgxpool.Pool
}

// NewPgxPersister builds a persister backed by a pgx pool.
func NewPgxPersister(pool *pgxpool.Pool) *PgxPersister {
	return &PgxPersister{pool: pool}
}

// PersistInTx runs fn inside a transaction and stores the messages it returns.
// The whole operation is atomic: if persisting any message fails the
// transaction is rolled back and the original error is returned.
func (p *PgxPersister) PersistInTx(ctx context.Context, fn func(tx pgx.Tx) ([]Message, error)) (err error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("%w: begin transaction", err)
	}

	defer func() {
		if err == nil {
			return
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			err = fmt.Errorf("%w: rollback after: %w", rollbackErr, err)
		}
	}()

	messages, err := fn(tx)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		TableName, insertColumns,
	)

	for i := range messages {
		msg := &messages[i]

		headers, err := encodeHeaders(msg.Headers)
		if err != nil {
			return err
		}

		if _, err = tx.Exec(
			ctx, query,
			msg.ID, msg.EventType, msg.Payload, headers, msg.Exchange, msg.RoutingKey, msg.PartitionKey,
		); err != nil {
			return fmt.Errorf("%w: persist messages", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit transaction", err)
	}

	return nil
}

// GormPersister persists outbox messages within a gorm transaction.
type GormPersister struct {
	db *gorm.DB
}

// NewGormPersister builds a persister backed by a gorm connection.
func NewGormPersister(db *gorm.DB) *GormPersister {
	return &GormPersister{db: db}
}

// PersistInTx runs fn inside a gorm transaction and stores the messages it
// returns. gorm rolls the transaction back automatically when fn or any insert
// returns an error.
func (p *GormPersister) PersistInTx(fn func(tx *gorm.DB) ([]Message, error)) error {
	return p.db.Transaction(func(tx *gorm.DB) error {
		messages, err := fn(tx)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (?, ?, ?, ?, ?, ?, ?)",
			TableName, insertColumns,
		)

		for i := range messages {
			msg := &messages[i]

			headers, err := encodeHeaders(msg.Headers)
			if err != nil {
				return err
			}

			if err = tx.Exec(
				query,
				msg.ID, msg.EventType, msg.Payload, headers, msg.Exchange, msg.RoutingKey, msg.PartitionKey,
			).Error; err != nil {
				return fmt.Errorf("%w: persist messages", err)
			}
		}

		return nil
	})
}
