# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-10-13

### Added

- Batch publishing: `EnqueueBatch` inserts up to `MaxBatchSize` (1000) jobs in
  one transaction. The batch is all-or-nothing; returned IDs match the input
  order, and a zero ID marks a job that already existed. `ErrBatchTooLarge`
  reports a size above the limit. Metrics fire for the inserted jobs after the
  commit.
- Batch claiming: `ClaimBatch` claims up to `MaxBatchSize` due jobs with one
  `FOR UPDATE SKIP LOCKED ... LIMIT` statement and the same leases and attempt
  fencing as `ClaimNext`. With ordered aggregates it falls back to repeated
  single ordered claims, so one call returns at most one job per aggregate.
- Worker concurrency: `WorkerConfig.Concurrency` runs parallel claim loops and
  `WorkerConfig.ClaimBatchSize` claims jobs in batches per statement. Shutdown
  still drains in-flight handlers before `Run` returns.
- Prometheus adapter: the new `github.com/m1chlcz/pgoutbox/prometheus`
  subpackage implements `Metrics` with per-kind counters and a pending gauge.
  The core package stays free of the Prometheus dependency.
- Tests for batch atomicity, batch claims under concurrent workers, lease and
  fencing through `ClaimBatch`, ordered batch claims, worker concurrency and
  handler draining, batch metric transitions, and the adapter collectors.

### Changed

- `WorkerConfig` gains `Concurrency` (default 1) and `ClaimBatchSize`
  (default 0, single-job claims); the zero values keep the previous behavior.
- `NewWorker` rejects a negative concurrency and a claim batch size outside
  zero to `MaxBatchSize` with `ErrWorkerConfig`.

## [0.2.0] - 2026-11-01

### Added

- Metrics hooks: the `Metrics` interface with `Published`, `Delivered`,
  `Failed`, `Retried`, `DeadLettered`, and `Pending` callbacks, the
  `NoopMetrics` default, and the `WithMetrics` queue option. The callbacks
  fire from the publish and worker paths after the database transition
  succeeds.
- Dead-letter operations: `ListDeadLetters` returns dead-lettered jobs with
  their payload, attempt count, and last error; `Redeliver` resets a dead
  letter's attempts and state and makes it due immediately. `Retry` remains as
  an alias.
- Delayed messages: `EnqueueInput.ScheduledAt` and the `scheduled_at` column.
  A claim requires both `available_at` and `scheduled_at` to be due, so a
  retry backoff still applies after a delayed job becomes due. `EnsureSchema`
  adds the column to existing tables.
- Tests for metric transitions, fenced attempts, dead-letter listing and
  redelivery, delayed claimability, and concurrent workers delivering each
  job once.

### Changed

- `List` and `JobView` expose the new `scheduled_at` column.
- `Drop` reports the pending gauge through `Metrics.Pending`.
