// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

// DBAdapter abstracts the minimal database surface the repository needs, so the
// same query logic works over pgx and gorm.
type DBAdapter interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
}

// Rows is an iterator over a query result set.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

// pgxRows adapts pgx.Rows to the Rows interface.
type pgxRows struct {
	rows pgx.Rows
}

func (r pgxRows) Next() bool {
	return r.rows.Next()
}

func (r pgxRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r pgxRows) Close() error {
	r.rows.Close()

	return nil
}

// PGXAdapter implements DBAdapter on top of a pgx connection pool.
type PGXAdapter struct {
	pool *pgxpool.Pool
}

// NewPGXAdapter wraps a pgx pool as a DBAdapter.
func NewPGXAdapter(pool *pgxpool.Pool) *PGXAdapter {
	return &PGXAdapter{pool: pool}
}

// Query runs a query and returns an iterator over the result set.
func (a *PGXAdapter) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := a.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return pgxRows{rows: rows}, nil
}

// Exec runs a statement that does not return rows.
func (a *PGXAdapter) Exec(ctx context.Context, query string, args ...any) error {
	if _, err := a.pool.Exec(ctx, query, args...); err != nil {
		return err
	}

	return nil
}

// GORMAdapter implements DBAdapter on top of a gorm connection.
type GORMAdapter struct {
	db *gorm.DB
}

// NewGORMAdapter wraps a gorm connection as a DBAdapter.
func NewGORMAdapter(db *gorm.DB) *GORMAdapter {
	return &GORMAdapter{db: db}
}

// Query runs a query and returns an iterator over the result set.
func (a *GORMAdapter) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return a.db.WithContext(ctx).Raw(query, args...).Rows()
}

// Exec runs a statement that does not return rows.
func (a *GORMAdapter) Exec(ctx context.Context, query string, args ...any) error {
	return a.db.WithContext(ctx).Exec(query, args...).Error
}
