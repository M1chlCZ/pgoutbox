//nolint:paralleltest // Integration tests share one PostgreSQL database and truncate it.
package pgoutbox_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m1chlcz/pgoutbox"
)

func testQueue(t *testing.T, options ...pgoutbox.Option) *pgoutbox.Queue {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err = pgoutbox.EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err = pool.Exec(ctx, "TRUNCATE outbox_jobs"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	queue, err := pgoutbox.New(pool, options...)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	return queue
}

func input(kind string, aggregateID uuid.UUID) pgoutbox.EnqueueInput {
	return pgoutbox.EnqueueInput{
		Kind:          kind,
		AggregateType: "widget",
		AggregateID:   aggregateID,
		Payload:       []byte(`{"value":1}`),
	}
}

func mustEnqueue(t *testing.T, queue *pgoutbox.Queue, kind string, aggregate uuid.UUID) {
	t.Helper()
	created, err := queue.Enqueue(context.Background(), input(kind, aggregate))
	if err != nil || !created {
		t.Fatalf("enqueue %s = %t, %v", kind, created, err)
	}
}

func mustClaim(t *testing.T, queue *pgoutbox.Queue, kind, workerID string) (pgoutbox.Job, bool) {
	t.Helper()
	job, found, err := queue.ClaimNext(context.Background(), []string{kind}, workerID)
	if err != nil {
		t.Fatalf("claim %s: %v", kind, err)
	}
	return job, found
}

func mustFoundClaim(t *testing.T, queue *pgoutbox.Queue, kind, workerID string) pgoutbox.Job {
	t.Helper()
	job, found := mustClaim(t, queue, kind, workerID)
	if !found {
		t.Fatalf("no %s job was due", kind)
	}
	return job
}

func mustComplete(t *testing.T, queue *pgoutbox.Queue, job pgoutbox.Job) bool {
	t.Helper()
	completed, err := queue.Complete(context.Background(), job)
	if err != nil {
		t.Fatalf("complete job %d: %v", job.ID, err)
	}
	return completed
}

func mustFail(t *testing.T, queue *pgoutbox.Queue, job pgoutbox.Job, reason string) bool {
	t.Helper()
	failed, err := queue.Fail(context.Background(), job, reason)
	if err != nil {
		t.Fatalf("fail job %d: %v", job.ID, err)
	}
	return failed
}

func mustRetry(t *testing.T, queue *pgoutbox.Queue, id int64) bool {
	t.Helper()
	retried, err := queue.Retry(context.Background(), id)
	if err != nil {
		t.Fatalf("retry job %d: %v", id, err)
	}
	return retried
}

func mustStats(t *testing.T, queue *pgoutbox.Queue, kinds []string) pgoutbox.Stats {
	t.Helper()
	stats, err := queue.Stats(context.Background(), kinds)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return stats
}

func mustStatsEqual(t *testing.T, queue *pgoutbox.Queue, kinds []string, want pgoutbox.Stats) {
	t.Helper()
	if got := mustStats(t, queue, kinds); got != want {
		t.Fatalf("stats = %#v, want %#v", got, want)
	}
}

func mustList(
	t *testing.T,
	queue *pgoutbox.Queue,
	state pgoutbox.State,
	kinds []string,
	limit int,
) []pgoutbox.JobView {
	t.Helper()
	views, err := queue.List(context.Background(), state, kinds, limit)
	if err != nil {
		t.Fatalf("list %s: %v", state, err)
	}
	return views
}

func mustDrop(t *testing.T, queue *pgoutbox.Queue, id int64) bool {
	t.Helper()
	dropped, err := queue.Drop(context.Background(), id)
	if err != nil {
		t.Fatalf("drop job %d: %v", id, err)
	}
	return dropped
}

func mustWorker(t *testing.T, config pgoutbox.WorkerConfig) *pgoutbox.Worker {
	t.Helper()
	worker, err := pgoutbox.NewWorker(config)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return worker
}

func mustProcess(t *testing.T, worker *pgoutbox.Worker) bool {
	t.Helper()
	processed, err := worker.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("process next: %v", err)
	}
	return processed
}

func TestEnqueueClaimComplete(t *testing.T) {
	ctx := context.Background()
	queue := testQueue(t)
	aggregate := uuid.New()
	mustEnqueue(t, queue, "export", aggregate)
	duplicate, err := queue.Enqueue(ctx, input("export", aggregate))
	if err != nil || duplicate {
		t.Fatalf("duplicate enqueue = %t, %v", duplicate, err)
	}
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if job.Attempts != 1 || job.Kind != "export" || job.AggregateID != aggregate {
		t.Fatalf("job = %#v", job)
	}
	if !mustComplete(t, queue, job) {
		t.Fatal("complete returned false")
	}
	if _, found := mustClaim(t, queue, "export", "worker-1"); found {
		t.Fatal("processed job was claimed again")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{})
}

func TestLeaseExpiryReclaimsJob(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithLease(60*time.Millisecond))
	mustEnqueue(t, queue, "export", uuid.New())
	first := mustFoundClaim(t, queue, "export", "worker-1")
	if _, found := mustClaim(t, queue, "export", "worker-2"); found {
		t.Fatal("leased job was claimed twice")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Leased: 1})
	time.Sleep(100 * time.Millisecond)
	second := mustFoundClaim(t, queue, "export", "worker-2")
	if second.ID != first.ID || second.Attempts != 2 || second.WorkerID != "worker-2" {
		t.Fatalf("reclaimed job = %#v", second)
	}
}

func TestAttemptFencingRejectsStaleWorkers(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithLease(60*time.Millisecond))
	mustEnqueue(t, queue, "export", uuid.New())
	stale := mustFoundClaim(t, queue, "export", "worker-1")
	time.Sleep(100 * time.Millisecond)
	fresh := mustFoundClaim(t, queue, "export", "worker-2")
	if mustComplete(t, queue, stale) {
		t.Fatal("stale worker completed the job")
	}
	if mustFail(t, queue, stale, "stale") {
		t.Fatal("stale worker failed the job")
	}
	if !mustComplete(t, queue, fresh) {
		t.Fatal("fresh worker did not complete the job")
	}
}

func TestFailSchedulesRetry(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithBackoff(pgoutbox.Backoff{
		Base: 20 * time.Millisecond, Max: 20 * time.Millisecond, ShiftMax: 0,
	}))
	mustEnqueue(t, queue, "export", uuid.New())
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, job, "temporary outage") {
		t.Fatal("fail returned false")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Pending: 1})
	if _, found := mustClaim(t, queue, "export", "worker-1"); found {
		t.Fatal("retried job was due immediately")
	}
	time.Sleep(40 * time.Millisecond)
	retried := mustFoundClaim(t, queue, "export", "worker-1")
	if retried.ID != job.ID || retried.Attempts != 2 {
		t.Fatalf("retried job = %#v", retried)
	}
}

func TestFailDeadLettersAtLimit(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithMaxAttempts(1))
	mustEnqueue(t, queue, "export", uuid.New())
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, job, "permanent failure") {
		t.Fatal("fail returned false")
	}
	if _, found := mustClaim(t, queue, "export", "worker-1"); found {
		t.Fatal("dead-lettered job was claimed")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Dead: 1})
	if !mustRetry(t, queue, job.ID) {
		t.Fatal("retry returned false")
	}
	reclaimed := mustFoundClaim(t, queue, "export", "worker-1")
	if reclaimed.Attempts != 1 {
		t.Fatalf("reclaimed job = %#v", reclaimed)
	}
	mustComplete(t, queue, reclaimed)
}

func TestWorkerCompletesAndDrops(t *testing.T) {
	queue := testQueue(t)
	handler := func(_ context.Context, job pgoutbox.Job) error {
		if job.Kind == "drop" {
			return fmt.Errorf("permanent: %w", pgoutbox.ErrDrop)
		}
		return nil
	}
	worker := mustWorker(t, pgoutbox.WorkerConfig{
		Queue: queue, Kinds: []string{"export", "drop"}, Handler: handler, WorkerID: "worker-1",
	})
	mustEnqueue(t, queue, "export", uuid.New())
	if !mustProcess(t, worker) {
		t.Fatal("worker did not process the export job")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{})
	mustEnqueue(t, queue, "drop", uuid.New())
	if !mustProcess(t, worker) {
		t.Fatal("worker did not process the drop job")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Dead: 1})
}

func TestOrderedAggregatesWaitForPredecessors(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithOrderedAggregates(true))
	aggregate := uuid.New()
	mustEnqueue(t, queue, "step", aggregate)
	nextInput := input("step", aggregate)
	nextInput.Generation = 1
	created, err := queue.Enqueue(context.Background(), nextInput)
	if err != nil || !created {
		t.Fatalf("enqueue second = %t, %v", created, err)
	}
	job := mustFoundClaim(t, queue, "step", "worker-1")
	if _, found := mustClaim(t, queue, "step", "worker-2"); found {
		t.Fatal("later job was claimed before its predecessor")
	}
	if !mustComplete(t, queue, job) {
		t.Fatal("complete returned false")
	}
	next := mustFoundClaim(t, queue, "step", "worker-2")
	if next.ID <= job.ID {
		t.Fatalf("second claim = %#v", next)
	}
}

func TestListAndDrop(t *testing.T) {
	queue := testQueue(t)
	first, err := queue.Enqueue(context.Background(), input("export", uuid.New()))
	if err != nil || !first {
		t.Fatalf("enqueue first = %t, %v", first, err)
	}
	mustEnqueue(t, queue, "export", uuid.New())
	views := mustList(t, queue, pgoutbox.StatePending, nil, 10)
	if len(views) != 2 {
		t.Fatalf("pending list = %d", len(views))
	}
	if !mustDrop(t, queue, views[0].ID) {
		t.Fatal("drop returned false")
	}
	mustStatsEqual(t, queue, []string{"export"}, pgoutbox.Stats{Pending: 1})
	if exportViews := mustList(t, queue, pgoutbox.StatePending, []string{"export"}, 10); len(exportViews) != 1 {
		t.Fatalf("export list = %d", len(exportViews))
	}
	if deadViews := mustList(t, queue, pgoutbox.StateDead, []string{"export"}, 10); len(deadViews) != 0 {
		t.Fatalf("dead list = %d", len(deadViews))
	}
}

func TestConcurrentClaimsAreExclusive(t *testing.T) {
	ctx := context.Background()
	queue := testQueue(t)
	const jobs = 20
	for range jobs {
		mustEnqueue(t, queue, "export", uuid.New())
	}
	var wait sync.WaitGroup
	claimed := make(chan int64, jobs)
	for worker := range 8 {
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			for {
				job, found, err := queue.ClaimNext(ctx, []string{"export"}, id)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !found {
					return
				}
				claimed <- job.ID
			}
		}(fmt.Sprintf("worker-%d", worker))
	}
	wait.Wait()
	close(claimed)
	seen := make(map[int64]bool, jobs)
	for id := range claimed {
		if seen[id] {
			t.Fatalf("job %d was claimed twice", id)
		}
		seen[id] = true
	}
	if len(seen) != jobs {
		t.Fatalf("claimed %d of %d jobs", len(seen), jobs)
	}
}

func TestWorkerRunStopsOnCancel(t *testing.T) {
	queue := testQueue(t)
	processed := make(chan struct{}, 1)
	worker := mustWorker(t, pgoutbox.WorkerConfig{
		Queue:        queue,
		Kinds:        []string{"export"},
		WorkerID:     "worker-1",
		IdleInterval: 10 * time.Millisecond,
		Handler: func(context.Context, pgoutbox.Job) error {
			processed <- struct{}{}
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if _, err := queue.Enqueue(ctx, input("export", uuid.New())); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not process the job")
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("Run error = %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
}
