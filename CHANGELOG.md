# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
