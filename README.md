# pgoutbox

A PostgreSQL-backed job queue in one table: atomic claims with leases, attempt fencing, retries with backoff, and dead-lettering.

## Features

- One table, `outbox_jobs`. A unique key on kind, aggregate type, aggregate ID, and generation makes a repeated enqueue a no-op.
- Claims use `FOR UPDATE SKIP LOCKED`, so many workers claim in parallel without blocking each other.
- Leases. A crashed worker leaves a job that another worker reclaims after the lease ends.
- Attempt fencing. Only the worker that holds the current attempt and lease can complete or fail a job.
- Retries with exponential backoff and a cap. A job that reaches the attempt limit is dead-lettered.
- Optional ordered aggregates: one job per aggregate at a time.
- `outboxctl` for operations: stats, list, retry, and drop.

## Install

```
go get github.com/m1chlcz/pgoutbox
```

Go 1.27 or later is required.

## Schema

Create the table once:

```go
if err := pgoutbox.EnsureSchema(ctx, pool); err != nil {
	return err
}
```

For production, apply `schema.sql` with your migration tool instead.

## Quick start

```go
queue, err := pgoutbox.New(pool)

_, err = queue.Enqueue(ctx, pgoutbox.EnqueueInput{
	Kind:          "export",
	AggregateType: "document",
	AggregateID:   documentID,
	Payload:       []byte(`{"format":"csv"}`),
})

worker, err := pgoutbox.NewWorker(pgoutbox.WorkerConfig{
	Queue:    queue,
	Kinds:    []string{"export"},
	WorkerID: "worker-1",
	Handler: func(ctx context.Context, job pgoutbox.Job) error {
		return exportDocument(ctx, job)
	},
})
err = worker.Run(ctx)
```

A nil handler error completes the job. Any other error schedules a retry with backoff. Wrap `ErrDrop` to dead-letter the job at once:

```go
return fmt.Errorf("bad payload: %w", pgoutbox.ErrDrop)
```

A handler panic is not recovered. The lease expires, and another worker retries the job.

## Queue options

| Option | Default | Meaning |
| --- | --- | --- |
| `WithMaxAttempts` | 10 | Claims before the job is dead-lettered. |
| `WithLease` | 1 minute | Lease duration. |
| `WithBackoff` | 30s to 30m | Retry delay. |
| `WithOrderedAggregates` | false | One job per aggregate at a time. |

## Worker defaults

| Setting | Default |
| --- | --- |
| `IdleInterval` | 1 second |
| `ErrorInterval` | 5 seconds |
| `JobTimeout` | 1 minute |

## Operations

```
outboxctl --dsn "$DATABASE_URL" stats
outboxctl --dsn "$DATABASE_URL" list --state dead --limit 20
outboxctl --dsn "$DATABASE_URL" retry --id 42
outboxctl --dsn "$DATABASE_URL" drop --id 42
```

The CLI reads `DATABASE_URL` when `--dsn` is absent.

## Table columns

`id`, `kind`, `aggregate_type`, `aggregate_id`, `generation`, `payload`, `attempts`, `available_at`, `leased_at`, `lease_until`, `leased_by`, `processed_at`, `dead_lettered_at`, `last_error`, `created_at`.

## Development

```
go test ./...
```

The integration tests need PostgreSQL. Set `TEST_DATABASE_URL`; without it they skip.

## License

MIT. See `LICENSE`.
