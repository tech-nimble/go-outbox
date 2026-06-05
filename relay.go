// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/avast/retry-go"
)

// iterationTimeout bounds a single fetch/publish/mark cycle so a stuck broker
// cannot block the relay loop indefinitely.
const iterationTimeout = 30 * time.Second

// partitionBuffer is the per-partition channel capacity.
const partitionBuffer = 1000

// Relay polls the outbox table and publishes pending messages, fanning them out
// across a fixed number of partitions to preserve per-key ordering while
// publishing concurrently.
type Relay struct {
	repository EventRepository
	publisher  Publisher
	delay      time.Duration
	partitions int
}

// NewRelay builds a relay. partitions controls the publishing concurrency and
// delay is the pause between polling iterations.
func NewRelay(repo EventRepository, publisher Publisher, partitions int, delay time.Duration) *Relay {
	return &Relay{
		repository: repo,
		publisher:  publisher,
		delay:      delay,
		partitions: partitions,
	}
}

// Run polls until the context is canceled. It validates batchSize once up
// front and returns nil on a clean shutdown.
func (r *Relay) Run(ctx context.Context, batchSize BatchSize) error {
	if err := batchSize.Valid(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		r.iterate(ctx, batchSize)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.delay):
		}
	}
}

// iterate runs a single fetch -> partition -> publish -> mark cycle.
func (r *Relay) iterate(ctx context.Context, batchSize BatchSize) {
	ctx, cancel := context.WithTimeout(ctx, iterationTimeout)
	defer cancel()

	fetched := r.repository.Fetch(ctx, batchSize)
	partitioned := r.fanOut(ctx, fetched)
	published := r.fanInPublish(ctx, partitioned)
	r.markConsumed(ctx, published, batchSize)
}

// fanOut distributes messages across partition channels by partition key,
// keeping messages with the same key on the same channel.
func (r *Relay) fanOut(ctx context.Context, in <-chan Message) []chan Message {
	out := make([]chan Message, r.partitions)
	for i := range out {
		out[i] = make(chan Message, partitionBuffer)
	}

	go func() {
		defer func() {
			for _, ch := range out {
				close(ch)
			}
		}()

		for msg := range orDone(ctx, in) {
			idx := int(msg.PartitionKey.Int64) % len(out)
			if idx < 0 {
				idx += len(out)
			}

			select {
			case out[idx] <- msg:
			case <-ctx.Done():
				return
			default:
				logger.Warn().
					Int("partition", idx).
					Str("message_id", msg.ID).
					Msg("partition channel full, message left for next iteration")
			}
		}
	}()

	return out
}

// fanInPublish publishes each partition concurrently and merges the messages
// that were delivered successfully into a single channel.
func (r *Relay) fanInPublish(ctx context.Context, partitions []chan Message) <-chan Message {
	out := make(chan Message, partitionBuffer)

	go func() {
		defer close(out)

		var wg sync.WaitGroup
		wg.Add(len(partitions))

		for _, partition := range partitions {
			go func(in <-chan Message) {
				defer wg.Done()

				for msg := range orDone(ctx, in) {
					if err := r.publish(ctx, &msg); err != nil {
						logger.Error().Err(err).Str("message_id", msg.ID).Msg("while publishing message")

						continue
					}

					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				}
			}(partition)
		}

		wg.Wait()
	}()

	return out
}

// publish delivers a single message with bounded retries. The message is taken
// by pointer to avoid copying it; it is dereferenced when handed to the
// Publisher, whose contract accepts the message by value.
func (r *Relay) publish(ctx context.Context, msg *Message) error {
	return retry.Do(
		func() error {
			return r.publisher.Publish(msg.Exchange, msg.RoutingKey, *msg)
		},
		retry.Context(ctx),
		retry.Delay(PublishRetryDelay),
		retry.Attempts(PublishRetryAttempts),
	)
}

// markConsumed collects published messages and marks them consumed in one call.
func (r *Relay) markConsumed(ctx context.Context, in <-chan Message, batchSize BatchSize) {
	msgs := make([]Message, 0, batchSize)
	for msg := range orDone(ctx, in) {
		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		return
	}

	if err := r.repository.MarkConsumed(ctx, msgs); err != nil {
		logger.Error().Err(err).
			Int("message_count", len(msgs)).
			Msg("failed to mark messages consumed, they will be reprocessed")
	}
}
