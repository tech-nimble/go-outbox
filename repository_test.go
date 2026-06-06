// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRows iterates a predefined set of messages.
type fakeRows struct {
	data []Message
	pos  int
}

func (r *fakeRows) Next() bool {
	r.pos++

	return r.pos <= len(r.data)
}

func (r *fakeRows) Scan(dest ...any) error {
	msg := r.data[r.pos-1]
	*dest[0].(*string) = msg.ID
	*dest[1].(*string) = msg.EventType
	*dest[2].(*string) = msg.Exchange
	*dest[3].(*string) = msg.RoutingKey
	*dest[5].(*any) = msg.Payload

	if encoded, err := encodeHeaders(msg.Headers); err == nil {
		*dest[6].(*any) = encoded
	}

	return nil
}

func (r *fakeRows) Close() error { return nil }

// fakeAdapter is an in-memory DBAdapter capturing Exec calls.
type fakeAdapter struct {
	rows     []Message
	queryErr error
	execArgs [][]any
}

func (a *fakeAdapter) Query(_ context.Context, _ string, _ ...any) (Rows, error) {
	if a.queryErr != nil {
		return nil, a.queryErr
	}

	return &fakeRows{data: a.rows}, nil
}

func (a *fakeAdapter) Exec(_ context.Context, _ string, args ...any) error {
	a.execArgs = append(a.execArgs, args)

	return nil
}

func TestRepositoryFetch(t *testing.T) {
	t.Parallel()

	adapter := &fakeAdapter{rows: []Message{
		{ID: "1", EventType: "A"},
		{ID: "2", EventType: "B"},
	}}
	repo := NewRepository(adapter)

	var got []Message
	for msg := range repo.Fetch(context.Background(), BatchSize(10)) {
		got = append(got, msg)
	}

	require.Len(t, got, 2)
	assert.Equal(t, "1", got[0].ID)
	assert.Equal(t, "2", got[1].ID)
}

func TestRepositoryFetchDecodesHeaders(t *testing.T) {
	t.Parallel()

	adapter := &fakeAdapter{rows: []Message{
		{ID: "1", Headers: map[string]any{"trace_id": "t-1"}},
		{ID: "2"},
	}}
	repo := NewRepository(adapter)

	var got []Message
	for msg := range repo.Fetch(context.Background(), BatchSize(10)) {
		got = append(got, msg)
	}

	require.Len(t, got, 2)
	assert.Equal(t, map[string]any{"trace_id": "t-1"}, got[0].Headers)
	assert.Nil(t, got[1].Headers)
}

func TestRepositoryFetchQueryError(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeAdapter{queryErr: errors.New("boom")})

	count := 0
	for range repo.Fetch(context.Background(), BatchSize(10)) {
		count++
	}

	assert.Zero(t, count)
}

func TestRepositoryMarkConsumed(t *testing.T) {
	t.Parallel()

	adapter := &fakeAdapter{}
	repo := NewRepository(adapter)

	err := repo.MarkConsumed(context.Background(), []Message{{ID: "1"}, {ID: ""}, {ID: "2"}})

	require.NoError(t, err)
	require.Len(t, adapter.execArgs, 1)
	// args: consumed flag, ids slice. Empty id is skipped.
	assert.Equal(t, []string{"1", "2"}, adapter.execArgs[0][1])
}

func TestRepositoryMarkConsumedEmpty(t *testing.T) {
	t.Parallel()

	adapter := &fakeAdapter{}
	repo := NewRepository(adapter)

	require.NoError(t, repo.MarkConsumed(context.Background(), nil))
	assert.Empty(t, adapter.execArgs)
}
