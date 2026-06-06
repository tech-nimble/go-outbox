// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRelayRunInvalidBatchSize(t *testing.T) {
	t.Parallel()

	relay := NewRelay(&repositoryMock{}, &publisherMock{}, 1, time.Millisecond)

	err := relay.Run(context.Background(), 0)

	assert.ErrorIs(t, err, ErrBatchSizeOutOfRange)
}

func TestRelayPublishesAndMarksConsumed(t *testing.T) {
	t.Parallel()

	repo := &repositoryMock{messages: generateMessages(50)}
	publisher := &publisherMock{}
	relay := NewRelay(repo, publisher, 4, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = relay.Run(ctx, BatchSize(100))
	}()

	assert.Eventually(t, func() bool {
		return publisher.count() == 50 && len(repo.consumedIDs()) == 50
	}, time.Second, 5*time.Millisecond)

	cancel()
	<-done
}

func generateMessages(n int) []Message {
	msgs := make([]Message, n)
	for i := range msgs {
		msgs[i] = NewMessage(
			"id-"+itoa(i), "Test", map[string]int{"num": i}, "exchange", itoa(i), "routing",
		)
	}

	return msgs
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}

	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}

	return string(b[pos:])
}
