// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repositoryMock is an in-memory EventRepository for tests.
type repositoryMock struct {
	mu       sync.Mutex
	messages []Message
	consumed []string
}

func (m *repositoryMock) Fetch(ctx context.Context, _ BatchSize) <-chan Message {
	ch := make(chan Message)

	m.mu.Lock()
	batch := m.messages
	m.messages = nil
	m.mu.Unlock()

	go func() {
		defer close(ch)

		for _, msg := range batch {
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (m *repositoryMock) MarkConsumed(_ context.Context, msgs []Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, msg := range msgs {
		m.consumed = append(m.consumed, msg.ID)
	}

	return nil
}

func (m *repositoryMock) consumedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, len(m.consumed))
	copy(out, m.consumed)

	return out
}

// publisherMock records every published message.
type publisherMock struct {
	mu        sync.Mutex
	published []Message
}

func (p *publisherMock) Publish(_, _ string, message Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.published = append(p.published, message)

	return nil
}

func (p *publisherMock) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.published)
}

func TestBatchSizeValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    BatchSize
		wantErr bool
	}{
		{name: "zero", size: 0, wantErr: true},
		{name: "one", size: 1, wantErr: false},
		{name: "max", size: maxBatchSize, wantErr: false},
		{name: "over max", size: maxBatchSize + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.size.Valid()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrBatchSizeOutOfRange)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestNewMessage(t *testing.T) {
	t.Parallel()

	msg := NewMessage("id-1", "Order", "payload", "orders", "key", "orders.created")

	assert.Equal(t, "id-1", msg.ID)
	assert.Equal(t, "Order", msg.EventType)
	assert.Equal(t, "orders", msg.Exchange)
	assert.Equal(t, "orders.created", msg.RoutingKey)
	assert.True(t, msg.PartitionKey.Valid)
	assert.False(t, msg.CreatedAt.IsZero())
}

func TestMessageBytePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload any
		want    []byte
		wantErr bool
	}{
		{name: "string", payload: "hello", want: []byte("hello")},
		{name: "bytes", payload: []byte("world"), want: []byte("world")},
		{name: "unsupported", payload: 42, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := Message{Payload: tt.payload}
			got, err := msg.BytePayload()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrUnsupportedPayload)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPartitionKeyDeterministic(t *testing.T) {
	t.Parallel()

	assert.Equal(t, partitionKey("same"), partitionKey("same"))
	assert.NotEqual(t, partitionKey("a"), partitionKey("b"))
}
