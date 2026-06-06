# go-outbox

[![CI](https://github.com/tech-nimble/go-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/tech-nimble/go-outbox/actions/workflows/ci.yml)

Transactional outbox for PostgreSQL with a polling relay. Messages are persisted
in the same transaction as your business data and later published to a broker,
guaranteeing at-least-once delivery without distributed transactions.

## Features

- Persist messages atomically with business data (pgx or gorm).
- Polling relay that publishes batches with bounded retries.
- Partitioned fan-out to publish concurrently while keeping per-key ordering.
- Optional per-message headers stored alongside the payload.
- Pluggable `Publisher` and `EventRepository`.
- Configurable table name, batch size, retry policy and partition algorithm.

## Install

```sh
go get github.com/tech-nimble/go-outbox
```

## Table schema

```sql
CREATE TABLE outbox_messages (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    headers       JSONB,
    exchange      TEXT        NOT NULL,
    routing_key   TEXT        NOT NULL,
    partition_key BIGINT,
    consumed      BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_unconsumed ON outbox_messages (created_at) WHERE consumed = FALSE;
```

`headers` is nullable: messages without headers store `NULL`. To migrate an
existing table, add the column:

```sql
ALTER TABLE outbox_messages ADD COLUMN headers JSONB;
```

## Usage

### Persist within a transaction (gorm)

```go
persister := outbox.NewGormPersister(db)

err := persister.PersistInTx(func(tx *gorm.DB) ([]outbox.Message, error) {
    // ... write business data via tx ...

    msg := outbox.NewMessage(
        uuid.NewString(), "OrderPaid", string(payload),
        "orders", "order-42", "orders.paid",
    )
    msg.Headers = map[string]any{"trace_id": traceID}

    return []outbox.Message{msg}, nil
})
```

### Run the relay

```go
repo := outbox.NewRepository(outbox.NewGORMAdapter(db))
relay := outbox.NewRelay(repo, publisher, 1_000, time.Millisecond)

if err := relay.Run(ctx, outbox.BatchSize(100)); err != nil {
    log.Fatal().Err(err).Msg("outbox relay stopped")
}
```

`publisher` implements:

```go
type Publisher interface {
    Publish(exchange, topic string, message outbox.Message) error
}
```

## Configuration

Package-level knobs, override once during bootstrap:

| Variable                       | Default            | Purpose                          |
| ------------------------------ | ------------------ | -------------------------------- |
| `outbox.TableName`             | `outbox_messages`  | Table the relay reads/writes.    |
| `outbox.PublishRetryDelay`     | `1s`               | Delay between publish retries.   |
| `outbox.PublishRetryAttempts`  | `3`                | Retries per message.             |
| `outbox.PartitionKeyAlgorithm` | FNV-1a             | Maps a partition string to a key.|

## Observability

The package logs through `zerolog`, tagging every line with
`component="outbox"` so logs can be filtered in Loki. The output format stays
under the application's control.

## License

[MIT](LICENSE) © Nimble Tech
