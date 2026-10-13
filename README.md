# pgoutbox

A PostgreSQL-backed job queue in one table: atomic claims with leases, attempt fencing, retries with backoff, dead-lettering, delayed messages, and metrics hooks.

## Features

- One table, `outbox_jobs`. A unique key on kind, aggregate type, aggregate ID, and generation makes a repeated enqueue a no-op.
- Batch publishing. `EnqueueBatch` inserts up to 1000 jobs in one transaction, all-or-nothing.
- Claims use `FOR UPDATE SKIP LOCKED`, so many workers claim in parallel without blocking each other. `ClaimBatch` claims many jobs with one statement.
- Leases. A crashed worker leaves a job that another worker reclaims after the lease ends.
- Attempt fencing. Only the worker that holds the current attempt and lease can complete or fail a job.
- Retries with exponential backoff and a cap. A job that reaches the attempt limit is dead-lettered.
- Dead letters stay in the table with their payload and last error. List them and redeliver them with a reset attempt count.
- Delayed messages. `EnqueueInput.ScheduledAt` makes a job claimable only when due.
- Optional ordered aggregates: one job per aggregate at a time.
- Worker concurrency. `Concurrency` parallel claim loops and an optional `ClaimBatchSize`, with the same lease and fencing guarantees.
- Optional `Metrics` hooks for counters and a pending gauge. The default is a no-op; no metrics library is required. A Prometheus adapter ships in the `prometheus` subpackage.
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

Version 0.2.0 adds an optional `scheduled_at timestamptz` column. No existing column changes. For a table created by 0.1.0, apply this migration once:

```sql
ALTER TABLE outbox_jobs ADD COLUMN IF NOT EXISTS scheduled_at timestamptz;
```

`EnsureSchema` runs the same statements, so it upgrades an existing table on its own. Dead letters need no new columns; `dead_lettered_at`, `attempts`, and `last_error` carry the state.

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

## Batch publishing

`EnqueueBatch` inserts many jobs in one transaction:

```go
ids, err := queue.EnqueueBatch(ctx, []pgoutbox.EnqueueInput{
	{Kind: "export", AggregateType: "document", AggregateID: firstID, Payload: []byte(`{"format":"csv"}`)},
	{Kind: "export", AggregateType: "document", AggregateID: secondID, Payload: []byte(`{"format":"pdf"}`)},
})
```

- One call takes at most `MaxBatchSize` (1000) jobs; more returns `ErrBatchTooLarge`.
- The batch is all-or-nothing. Any error rolls back every insert.
- `ids[i]` belongs to `inputs[i]` in input order. A zero ID means an existing job with the same dedupe key was left alone, exactly like `Enqueue`; a duplicate inside the batch also yields zero.
- Metrics fire per inserted job after the commit: `Published` then `Pending(kind, +1)` in input order.

## Batch claiming

`ClaimBatch` claims many due jobs with one statement:

```go
jobs, err := queue.ClaimBatch(ctx, []string{"export"}, "worker-1", 50)
```

- The limit must be between 1 and `MaxBatchSize`; more returns `ErrBatchTooLarge`.
- Claims use one `FOR UPDATE SKIP LOCKED ... LIMIT` statement, so leases and attempt fencing work exactly as in `ClaimNext`, and concurrent workers never receive the same job.
- With ordered aggregates, `ClaimBatch` falls back to repeated single ordered claims instead of one statement. One call then returns at most one job per aggregate, and later jobs still wait for their predecessors. A failure inside the fallback returns no jobs; jobs claimed before the failure stay leased until the lease expires.
- Keep a batch small enough to finish inside the lease. A lease that expires mid-batch lets another worker reclaim the unprocessed jobs.

## Queue options

| Option | Default | Meaning |
| --- | --- | --- |
| `WithMaxAttempts` | 10 | Claims before the job is dead-lettered. |
| `WithLease` | 1 minute | Lease duration. |
| `WithBackoff` | 30s to 30m | Retry delay. |
| `WithOrderedAggregates` | false | One job per aggregate at a time. |
| `WithMetrics` | `NoopMetrics` | Lifecycle counters and a pending gauge. |

## Worker defaults

| Setting | Default |
| --- | --- |
| `IdleInterval` | 1 second |
| `ErrorInterval` | 5 seconds |
| `JobTimeout` | 1 minute |
| `Concurrency` | 1 |
| `ClaimBatchSize` | 0 (off) |

`Concurrency` starts that many parallel claim loops when the worker runs. Every loop shares `WorkerID`; leases and attempt fencing keep them from double-processing a job. `ClaimBatchSize` from 2 to `MaxBatchSize` makes each loop claim a batch per statement; 0 or 1 keeps the single-job loop. Ordered aggregates always claim one job at a time. `Run` returns after every loop has stopped, so an in-flight handler finishes before shutdown completes.

## Metrics hooks

`WithMetrics` installs a `Metrics` implementation on the queue. Workers use the queue, so one hook sees both publish and worker transitions. The package never imports a metrics client; adapt the interface to your system.

Every callback is synchronous, inside the goroutine that performed the database change, after the statement reported the expected row change. An enqueue conflict or a fenced stale attempt fires nothing. Implementations must be safe for concurrent use, must not block, and must not call back into the queue.

| Callback | Fires when |
| --- | --- |
| `Published(kind)` | `Enqueue` inserted a new job, or one position of `EnqueueBatch` did. |
| `Delivered(kind)` | `Complete` marked a claimed job processed. |
| `Failed(kind)` | `Fail` returned a claimed job to pending with a backoff delay. A failure at the attempt limit calls `DeadLettered` instead. |
| `Retried(kind)` | `Redeliver` or `Retry` returned a dead-lettered job to pending. |
| `DeadLettered(kind)` | A claimed job entered the dead-letter state: `DeadLetter`, `Fail` at the attempt limit, or a worker error wrapping `ErrDrop`. |
| `Pending(kind, delta)` | The pending count changed: +1 on publish, retryable failure, and redelivery; -1 on a claim (`ClaimNext` or each job of `ClaimBatch`) and on dropping a pending job. A lease that expires without a later `Complete`, `Fail`, or `DeadLetter` call is not observed. |

A failed attempt calls `Failed` before `Pending`. Redelivery calls `Retried` before `Pending`. Embed `NoopMetrics` to override only the methods you need:

```go
type shopMetrics struct {
	pgoutbox.NoopMetrics
	published atomic.Int64
}

func (m *shopMetrics) Published(string) {
	m.published.Add(1)
}

queue, err := pgoutbox.New(pool, pgoutbox.WithMetrics(&shopMetrics{}))
```

## Prometheus adapter

The subpackage `github.com/m1chlcz/pgoutbox/prometheus` adapts `Metrics` to Prometheus collectors. The core package keeps no Prometheus dependency:

```go
import (
	"github.com/prometheus/client_golang/prometheus"

	outboxprometheus "github.com/m1chlcz/pgoutbox/prometheus"
)

adapter := outboxprometheus.New(prometheus.DefaultRegisterer, "pgoutbox")
queue, err := pgoutbox.New(pool, pgoutbox.WithMetrics(adapter))
```

With the namespace `pgoutbox` the adapter exports:

| Metric | Type |
| --- | --- |
| `pgoutbox_jobs_published_total{kind}` | counter |
| `pgoutbox_jobs_delivered_total{kind}` | counter |
| `pgoutbox_jobs_failed_total{kind}` | counter |
| `pgoutbox_jobs_retried_total{kind}` | counter |
| `pgoutbox_jobs_dead_lettered_total{kind}` | counter |
| `pgoutbox_jobs_pending{kind}` | gauge |

`New` panics when the registerer already has one of the collectors, so pass a fresh registry or the default registerer once. A nil registerer uses `prometheus.DefaultRegisterer`. An empty namespace omits the prefix.

## Dead letters

A job that reaches the attempt limit, is passed to `DeadLetter`, or returns an error wrapping `ErrDrop` keeps its payload, attempts, and last error. List the dead letters and redeliver one:

```go
letters, err := queue.ListDeadLetters(ctx, []string{"export"}, 20)
for _, letter := range letters {
	log.Printf("job %d failed %d times: %s", letter.ID, letter.Attempts, *letter.LastError)
}

changed, err := queue.Redeliver(ctx, id)
```

`ListDeadLetters` is `List` with `StateDead`; entries carry the payload and the last error. `Redeliver` clears `attempts`, `dead_lettered_at`, and `last_error` and makes the job due immediately. `Retry` is an alias. The CLI equivalents are `outboxctl list --state dead` and `outboxctl retry --id ID`.

## Delayed messages

Set `EnqueueInput.ScheduledAt` to make a job claimable only when due:

```go
due := time.Now().Add(24 * time.Hour)
_, err = queue.Enqueue(ctx, pgoutbox.EnqueueInput{
	Kind:          "reminder",
	AggregateType: "order",
	AggregateID:   orderID,
	Payload:       []byte(`{"channel":"email"}`),
	ScheduledAt:   &due,
})
```

A claim requires both `available_at` and `scheduled_at` to be due, so a retry backoff still applies after a delayed job fails. Before its due time the job counts as pending in `Stats` and appears in `List`. A worker that finds only future jobs takes its normal idle interval. A `ScheduledAt` value at or before now is due immediately. Nil keeps the default immediate behavior.

## Operations

```
outboxctl --dsn "$DATABASE_URL" stats
outboxctl --dsn "$DATABASE_URL" list --state dead --limit 20
outboxctl --dsn "$DATABASE_URL" retry --id 42
outboxctl --dsn "$DATABASE_URL" drop --id 42
```

The CLI reads `DATABASE_URL` when `--dsn` is absent.

## Table columns

`id`, `kind`, `aggregate_type`, `aggregate_id`, `generation`, `payload`, `attempts`, `available_at`, `scheduled_at`, `leased_at`, `lease_until`, `leased_by`, `processed_at`, `dead_lettered_at`, `last_error`, `created_at`.

## Development

```
go test ./...
```

The integration tests need PostgreSQL. Set `TEST_DATABASE_URL`; without it they skip.

## License

MIT. See `LICENSE`.
