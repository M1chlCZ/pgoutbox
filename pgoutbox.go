// Package pgoutbox implements a PostgreSQL-backed job queue: one outbox
// table, atomic claims with leases, attempt fencing, retries with backoff,
// and dead-lettering.
package pgoutbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	DefaultMaxAttempts = 10
	DefaultLease       = time.Minute
	maxReasonLength    = 500
)

var (
	ErrPoolRequired = errors.New("pgoutbox pool is required")
	ErrInvalidInput = errors.New("pgoutbox input is invalid")
	ErrWorkerConfig = errors.New("pgoutbox worker configuration is invalid")
	ErrDrop         = errors.New("pgoutbox drop job")
)

type Job struct {
	ID            int64
	Kind          string
	AggregateType string
	AggregateID   uuid.UUID
	Generation    int
	Payload       []byte
	Attempts      int
	WorkerID      string
}

type JobView struct {
	Job

	AvailableAt    time.Time
	LeasedAt       *time.Time
	LeaseUntil     *time.Time
	ProcessedAt    *time.Time
	DeadLetteredAt *time.Time
	LastError      *string
	CreatedAt      time.Time
}

type EnqueueInput struct {
	Kind          string
	AggregateType string
	AggregateID   uuid.UUID
	Generation    int
	Payload       []byte
}

type Stats struct {
	Pending int64
	Leased  int64
	Dead    int64
}

type State string

const (
	StateAll     State = "all"
	StatePending State = "pending"
	StateLeased  State = "leased"
	StateDead    State = "dead"
)

type Option func(*Queue) error

type Queue struct {
	pool        *pgxpool.Pool
	maxAttempts int
	lease       time.Duration
	backoff     Backoff
	ordered     bool
}

// EnsureSchema creates the outbox table and its indexes when they are absent.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return ErrPoolRequired
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("create outbox schema: %w", err)
	}
	return nil
}

// New returns a queue with the default attempts, lease, and backoff settings.
func New(pool *pgxpool.Pool, options ...Option) (*Queue, error) {
	if pool == nil {
		return nil, ErrPoolRequired
	}
	queue := &Queue{
		pool:        pool,
		maxAttempts: DefaultMaxAttempts,
		lease:       DefaultLease,
		backoff:     DefaultBackoff,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(queue); err != nil {
			return nil, err
		}
	}
	return queue, nil
}

// WithMaxAttempts sets the number of claims before a job is dead-lettered.
func WithMaxAttempts(attempts int) Option {
	return func(queue *Queue) error {
		if attempts < 1 || attempts > 1000 {
			return ErrInvalidInput
		}
		queue.maxAttempts = attempts
		return nil
	}
}

// WithLease sets the claim lease duration.
func WithLease(lease time.Duration) Option {
	return func(queue *Queue) error {
		if lease <= 0 {
			return ErrInvalidInput
		}
		queue.lease = lease
		return nil
	}
}

// WithBackoff sets the retry backoff.
func WithBackoff(backoff Backoff) Option {
	return func(queue *Queue) error {
		if err := backoff.validate(); err != nil {
			return err
		}
		queue.backoff = backoff
		return nil
	}
}

// WithOrderedAggregates claims one job per aggregate at a time. A job waits
// for every earlier job of the same kind and aggregate.
func WithOrderedAggregates(enabled bool) Option {
	return func(queue *Queue) error {
		queue.ordered = enabled
		return nil
	}
}

// Enqueue inserts one job. An existing job with the same kind, aggregate, and
// generation is left alone, and the second call returns false.
func (queue *Queue) Enqueue(ctx context.Context, input EnqueueInput) (bool, error) {
	if ctx == nil {
		return false, ErrInvalidInput
	}
	input.Kind = strings.TrimSpace(input.Kind)
	input.AggregateType = strings.TrimSpace(input.AggregateType)
	if input.Kind == "" || input.AggregateType == "" || input.AggregateID == uuid.Nil ||
		input.Generation < 0 || !validPayload(input.Payload) {
		return false, ErrInvalidInput
	}
	var id int64
	err := queue.pool.QueryRow(ctx, `
		INSERT INTO outbox_jobs (kind, aggregate_type, aggregate_id, generation, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (kind, aggregate_type, aggregate_id, generation) DO NOTHING
		RETURNING id
	`, input.Kind, input.AggregateType, input.AggregateID, input.Generation, input.Payload).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("enqueue job: %w", err)
	}
	return true, nil
}

// ClaimNext claims one due job of the given kinds with a lease. It returns
// false when no job is due.
func (queue *Queue) ClaimNext(ctx context.Context, kinds []string, workerID string) (Job, bool, error) {
	var job Job
	if ctx == nil || len(kinds) == 0 {
		return job, false, ErrInvalidInput
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 100 || strings.ContainsAny(workerID, "\r\n\x00") {
		return job, false, ErrInvalidInput
	}
	now := time.Now().UTC()
	err := queue.pool.QueryRow(ctx, claimSQL(queue.ordered),
		now, kinds, now.Add(queue.lease), workerID,
	).Scan(
		&job.ID,
		&job.Kind,
		&job.AggregateType,
		&job.AggregateID,
		&job.Generation,
		&job.Payload,
		&job.Attempts,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, false, nil
	}
	if err != nil {
		return job, false, fmt.Errorf("claim job: %w", err)
	}
	job.WorkerID = workerID
	return job, true, nil
}

// Complete marks a claimed job as processed. It returns false when the attempt
// or the lease no longer matches.
func (queue *Queue) Complete(ctx context.Context, job Job) (bool, error) {
	if ctx == nil || !validClaim(job) {
		return false, ErrInvalidInput
	}
	result, err := queue.pool.Exec(ctx, `
		UPDATE outbox_jobs
		SET processed_at = $3, leased_at = NULL, lease_until = NULL, leased_by = NULL, last_error = NULL
		WHERE id = $1 AND attempts = $2 AND leased_by = $4
		  AND processed_at IS NULL AND dead_lettered_at IS NULL
	`, job.ID, job.Attempts, time.Now().UTC(), job.WorkerID)
	if err != nil {
		return false, fmt.Errorf("complete job: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// Fail releases a claimed job with a backoff delay. The job is dead-lettered
// when it reached the attempt limit.
func (queue *Queue) Fail(ctx context.Context, job Job, reason string) (bool, error) {
	if ctx == nil || !validClaim(job) {
		return false, ErrInvalidInput
	}
	if job.Attempts >= queue.maxAttempts {
		return queue.DeadLetter(ctx, job, reason)
	}
	result, err := queue.pool.Exec(ctx, `
		UPDATE outbox_jobs
		SET available_at = $3, leased_at = NULL, lease_until = NULL, leased_by = NULL, last_error = $4
		WHERE id = $1 AND attempts = $2 AND leased_by = $5
		  AND processed_at IS NULL AND dead_lettered_at IS NULL
	`, job.ID, job.Attempts, time.Now().UTC().Add(queue.backoff.Delay(job.Attempts, job.ID)),
		boundedReason(reason), job.WorkerID)
	if err != nil {
		return false, fmt.Errorf("fail job: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// DeadLetter marks a claimed job as dead without another attempt.
func (queue *Queue) DeadLetter(ctx context.Context, job Job, reason string) (bool, error) {
	if ctx == nil || !validClaim(job) {
		return false, ErrInvalidInput
	}
	now := time.Now().UTC()
	result, err := queue.pool.Exec(ctx, `
		UPDATE outbox_jobs
		SET processed_at = $3, dead_lettered_at = $3, leased_at = NULL, lease_until = NULL,
		    leased_by = NULL, last_error = $4
		WHERE id = $1 AND attempts = $2 AND leased_by = $5
		  AND processed_at IS NULL AND dead_lettered_at IS NULL
	`, job.ID, job.Attempts, now, boundedReason(reason), job.WorkerID)
	if err != nil {
		return false, fmt.Errorf("dead-letter job: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// Stats counts pending, leased, and dead jobs. A pending job is due now or
// later; a leased job holds a live lease. An empty kinds list counts all jobs.
func (queue *Queue) Stats(ctx context.Context, kinds []string) (Stats, error) {
	var stats Stats
	if ctx == nil {
		return stats, ErrInvalidInput
	}
	err := queue.pool.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE processed_at IS NULL AND dead_lettered_at IS NULL
		                   AND (lease_until IS NULL OR lease_until <= now())),
		  count(*) FILTER (WHERE processed_at IS NULL AND dead_lettered_at IS NULL
		                   AND lease_until > now()),
		  count(*) FILTER (WHERE dead_lettered_at IS NOT NULL)
		FROM outbox_jobs
		WHERE $1::text[] IS NULL OR kind = ANY($1)
	`, optionalStrings(kinds)).Scan(&stats.Pending, &stats.Leased, &stats.Dead)
	if err != nil {
		return Stats{}, fmt.Errorf("read job statistics: %w", err)
	}
	return stats, nil
}

// List returns jobs for the given state, newest first.
func (queue *Queue) List(ctx context.Context, state State, kinds []string, limit int) ([]JobView, error) {
	if ctx == nil {
		return nil, ErrInvalidInput
	}
	if state == "" {
		state = StateAll
	}
	switch state {
	case StateAll, StatePending, StateLeased, StateDead:
	default:
		return nil, ErrInvalidInput
	}
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := queue.pool.Query(ctx, `
		SELECT id, kind, aggregate_type, aggregate_id, generation, payload, attempts,
		       available_at, leased_at, lease_until, leased_by,
		       processed_at, dead_lettered_at, last_error, created_at
		FROM outbox_jobs
		WHERE ($1::text[] IS NULL OR kind = ANY($1))
		  AND ($2 = 'all'
		       OR ($2 = 'pending' AND processed_at IS NULL AND dead_lettered_at IS NULL
		           AND (lease_until IS NULL OR lease_until <= now()))
		       OR ($2 = 'leased' AND processed_at IS NULL AND dead_lettered_at IS NULL
		           AND lease_until > now())
		       OR ($2 = 'dead' AND dead_lettered_at IS NOT NULL))
		ORDER BY id DESC
		LIMIT $3
	`, optionalStrings(kinds), string(state), limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	views := []JobView{}
	for rows.Next() {
		var view JobView
		var leasedBy *string
		if err = rows.Scan(
			&view.ID,
			&view.Kind,
			&view.AggregateType,
			&view.AggregateID,
			&view.Generation,
			&view.Payload,
			&view.Attempts,
			&view.AvailableAt,
			&view.LeasedAt,
			&view.LeaseUntil,
			&leasedBy,
			&view.ProcessedAt,
			&view.DeadLetteredAt,
			&view.LastError,
			&view.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		if leasedBy != nil {
			view.WorkerID = *leasedBy
		}
		views = append(views, view)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return views, nil
}

// Retry clears a dead-lettered job and makes it due again with a fresh attempt
// count.
func (queue *Queue) Retry(ctx context.Context, id int64) (bool, error) {
	if ctx == nil || id <= 0 {
		return false, ErrInvalidInput
	}
	result, err := queue.pool.Exec(ctx, `
		UPDATE outbox_jobs
		SET processed_at = NULL, dead_lettered_at = NULL, attempts = 0, available_at = now(),
		    leased_at = NULL, lease_until = NULL, leased_by = NULL, last_error = NULL
		WHERE id = $1 AND dead_lettered_at IS NOT NULL
	`, id)
	if err != nil {
		return false, fmt.Errorf("retry job: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

// Drop deletes one job.
func (queue *Queue) Drop(ctx context.Context, id int64) (bool, error) {
	if ctx == nil || id <= 0 {
		return false, ErrInvalidInput
	}
	result, err := queue.pool.Exec(ctx, `DELETE FROM outbox_jobs WHERE id = $1`, id)
	if err != nil {
		return false, fmt.Errorf("drop job: %w", err)
	}
	return result.RowsAffected() == 1, nil
}

func validPayload(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return false
	}
	return true
}

func validClaim(job Job) bool {
	return job.ID > 0 && job.Attempts > 0 && job.WorkerID != ""
}

func boundedReason(reason string) string {
	if len(reason) <= maxReasonLength {
		return reason
	}
	return strings.ToValidUTF8(reason[:maxReasonLength], "")
}

func optionalStrings(values []string) any {
	if len(values) == 0 {
		return nil
	}
	return values
}
