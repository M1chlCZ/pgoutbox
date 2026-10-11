//nolint:paralleltest // Integration tests share one PostgreSQL database and truncate it.
package pgoutbox_test

import (
	"context"
	"fmt"
	"os"
	"slices"
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

type metricEvent struct {
	Name  string
	Kind  string
	Delta int64
}

type recordingMetrics struct {
	mu     sync.Mutex
	events []metricEvent
}

func (metrics *recordingMetrics) record(name, kind string, delta int64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.events = append(metrics.events, metricEvent{Name: name, Kind: kind, Delta: delta})
}

func (metrics *recordingMetrics) Published(kind string) { metrics.record("published", kind, 0) }

func (metrics *recordingMetrics) Delivered(kind string) { metrics.record("delivered", kind, 0) }

func (metrics *recordingMetrics) Failed(kind string) { metrics.record("failed", kind, 0) }

func (metrics *recordingMetrics) Retried(kind string) { metrics.record("retried", kind, 0) }

func (metrics *recordingMetrics) DeadLettered(kind string) { metrics.record("dead_lettered", kind, 0) }

func (metrics *recordingMetrics) Pending(kind string, delta int64) {
	metrics.record("pending", kind, delta)
}

func (metrics *recordingMetrics) snapshot() []metricEvent {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return append([]metricEvent(nil), metrics.events...)
}

func (metrics *recordingMetrics) count(name string) int {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	total := 0
	for _, event := range metrics.events {
		if event.Name == name {
			total++
		}
	}
	return total
}

func mustMetricEvents(t *testing.T, metrics *recordingMetrics, want []metricEvent) {
	t.Helper()
	if got := metrics.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("metric events = %#v, want %#v", got, want)
	}
}

func TestMetricsFollowLifecycleTransitions(t *testing.T) {
	ctx := context.Background()
	metrics := &recordingMetrics{}
	queue := testQueue(t,
		pgoutbox.WithMetrics(metrics),
		pgoutbox.WithMaxAttempts(2),
		pgoutbox.WithBackoff(pgoutbox.Backoff{Base: 20 * time.Millisecond, Max: 20 * time.Millisecond}),
	)
	aggregate := uuid.New()
	created, err := queue.Enqueue(ctx, input("export", aggregate))
	if err != nil || !created {
		t.Fatalf("enqueue = %t, %v", created, err)
	}
	if duplicate, err := queue.Enqueue(ctx, input("export", aggregate)); err != nil || duplicate {
		t.Fatalf("duplicate enqueue = %t, %v", duplicate, err)
	}
	first := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, first, "first failure") {
		t.Fatal("fail returned false")
	}
	time.Sleep(40 * time.Millisecond)
	second := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, second, "second failure") {
		t.Fatal("fail returned false")
	}
	if !mustRetry(t, queue, first.ID) {
		t.Fatal("retry returned false")
	}
	redelivered := mustFoundClaim(t, queue, "export", "worker-1")
	if redelivered.ID != first.ID || redelivered.Attempts != 1 {
		t.Fatalf("redelivered job = %#v", redelivered)
	}
	if !mustComplete(t, queue, redelivered) {
		t.Fatal("complete returned false")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{})
	mustMetricEvents(t, metrics, []metricEvent{
		{Name: "published", Kind: "export"},
		{Name: "pending", Kind: "export", Delta: 1},
		{Name: "pending", Kind: "export", Delta: -1},
		{Name: "failed", Kind: "export"},
		{Name: "pending", Kind: "export", Delta: 1},
		{Name: "pending", Kind: "export", Delta: -1},
		{Name: "dead_lettered", Kind: "export"},
		{Name: "retried", Kind: "export"},
		{Name: "pending", Kind: "export", Delta: 1},
		{Name: "pending", Kind: "export", Delta: -1},
		{Name: "delivered", Kind: "export"},
	})
}

func TestMetricsIgnoreFencedAttempts(t *testing.T) {
	metrics := &recordingMetrics{}
	queue := testQueue(t, pgoutbox.WithMetrics(metrics), pgoutbox.WithLease(60*time.Millisecond))
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
	if delivered := metrics.count("delivered"); delivered != 1 {
		t.Fatalf("delivered callbacks = %d", delivered)
	}
	if failed := metrics.count("failed"); failed != 0 {
		t.Fatalf("failed callbacks = %d", failed)
	}
	if pending := metrics.count("pending"); pending != 3 {
		t.Fatalf("pending callbacks = %d", pending)
	}
}

func TestMetricsWorkerDeadLettersDroppedJob(t *testing.T) {
	metrics := &recordingMetrics{}
	queue := testQueue(t, pgoutbox.WithMetrics(metrics))
	worker := mustWorker(t, pgoutbox.WorkerConfig{
		Queue: queue, Kinds: []string{"drop"}, WorkerID: "worker-1",
		Handler: func(context.Context, pgoutbox.Job) error {
			return fmt.Errorf("permanent: %w", pgoutbox.ErrDrop)
		},
	})
	mustEnqueue(t, queue, "drop", uuid.New())
	if !mustProcess(t, worker) {
		t.Fatal("worker did not process the job")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Dead: 1})
	if deadLettered := metrics.count("dead_lettered"); deadLettered != 1 {
		t.Fatalf("dead_lettered callbacks = %d", deadLettered)
	}
	if failed := metrics.count("failed"); failed != 0 {
		t.Fatalf("failed callbacks = %d", failed)
	}
}

func TestMetricsDropPendingJob(t *testing.T) {
	metrics := &recordingMetrics{}
	queue := testQueue(t, pgoutbox.WithMetrics(metrics), pgoutbox.WithMaxAttempts(1))
	mustEnqueue(t, queue, "export", uuid.New())
	dead := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, dead, "permanent") {
		t.Fatal("fail returned false")
	}
	if !mustDrop(t, queue, dead.ID) {
		t.Fatal("drop dead job returned false")
	}
	mustEnqueue(t, queue, "export", uuid.New())
	pending := mustList(t, queue, pgoutbox.StatePending, nil, 10)
	if len(pending) != 1 {
		t.Fatalf("pending jobs = %d", len(pending))
	}
	if !mustDrop(t, queue, pending[0].ID) {
		t.Fatal("drop pending job returned false")
	}
	mustMetricEvents(t, metrics, []metricEvent{
		{Name: "published", Kind: "export"},
		{Name: "pending", Kind: "export", Delta: 1},
		{Name: "pending", Kind: "export", Delta: -1},
		{Name: "dead_lettered", Kind: "export"},
		{Name: "published", Kind: "export"},
		{Name: "pending", Kind: "export", Delta: 1},
		{Name: "pending", Kind: "export", Delta: -1},
	})
}

func TestDeadLetterListingAndRedelivery(t *testing.T) {
	ctx := context.Background()
	queue := testQueue(t, pgoutbox.WithMaxAttempts(1))
	aggregate := uuid.New()
	mustEnqueue(t, queue, "export", aggregate)
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, job, "permanent failure") {
		t.Fatal("fail returned false")
	}
	letters, err := queue.ListDeadLetters(ctx, nil, 10)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(letters) != 1 {
		t.Fatalf("dead letters = %d", len(letters))
	}
	letter := letters[0]
	if letter.ID != job.ID || letter.AggregateID != aggregate || letter.DeadLetteredAt == nil ||
		letter.LastError == nil || *letter.LastError != "permanent failure" || len(letter.Payload) == 0 {
		t.Fatalf("dead letter = %#v", letter)
	}
	if filtered, err := queue.ListDeadLetters(ctx, []string{"other"}, 10); err != nil || len(filtered) != 0 {
		t.Fatalf("filtered dead letters = %d, %v", len(filtered), err)
	}
	mustEnqueue(t, queue, "other", uuid.New())
	pending := mustFoundClaim(t, queue, "other", "worker-1")
	redelivered, err := queue.Redeliver(ctx, pending.ID)
	if err != nil {
		t.Fatalf("redeliver pending job: %v", err)
	}
	if redelivered {
		t.Fatal("redelivered a job that is not dead")
	}
	redelivered, err = queue.Redeliver(ctx, job.ID)
	if err != nil || !redelivered {
		t.Fatalf("redeliver = %t, %v", redelivered, err)
	}
	if empty, err := queue.ListDeadLetters(ctx, nil, 10); err != nil || len(empty) != 0 {
		t.Fatalf("dead letters after redelivery = %d, %v", len(empty), err)
	}
	reclaimed := mustFoundClaim(t, queue, "export", "worker-1")
	if reclaimed.ID != job.ID || reclaimed.Attempts != 1 {
		t.Fatalf("reclaimed job = %#v", reclaimed)
	}
	if !mustComplete(t, queue, reclaimed) {
		t.Fatal("complete returned false")
	}
}

func TestScheduledJobBecomesClaimableWhenDue(t *testing.T) {
	queue := testQueue(t)
	due := time.Now().UTC().Add(100 * time.Millisecond)
	scheduled := input("export", uuid.New())
	scheduled.ScheduledAt = &due
	created, err := queue.Enqueue(context.Background(), scheduled)
	if err != nil || !created {
		t.Fatalf("enqueue = %t, %v", created, err)
	}
	if _, found := mustClaim(t, queue, "export", "worker-1"); found {
		t.Fatal("scheduled job was claimed before it was due")
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{Pending: 1})
	worker := mustWorker(t, pgoutbox.WorkerConfig{
		Queue: queue, Kinds: []string{"export"}, WorkerID: "worker-1",
		Handler: func(context.Context, pgoutbox.Job) error { return nil },
	})
	if mustProcess(t, worker) {
		t.Fatal("worker processed a scheduled job before it was due")
	}
	time.Sleep(200 * time.Millisecond)
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if job.AggregateID != scheduled.AggregateID {
		t.Fatalf("claimed job = %#v", job)
	}
	views := mustList(t, queue, pgoutbox.StateAll, nil, 10)
	if len(views) != 1 || views[0].ScheduledAt == nil ||
		views[0].ScheduledAt.Sub(due).Abs() > time.Millisecond {
		t.Fatalf("scheduled view = %#v", views)
	}
}

func TestScheduledJobStillHonorsRetryBackoff(t *testing.T) {
	queue := testQueue(t, pgoutbox.WithBackoff(pgoutbox.Backoff{
		Base: 200 * time.Millisecond, Max: 200 * time.Millisecond,
	}))
	due := time.Now().UTC().Add(100 * time.Millisecond)
	scheduled := input("export", uuid.New())
	scheduled.ScheduledAt = &due
	if created, err := queue.Enqueue(context.Background(), scheduled); err != nil || !created {
		t.Fatalf("enqueue = %t, %v", created, err)
	}
	time.Sleep(200 * time.Millisecond)
	job := mustFoundClaim(t, queue, "export", "worker-1")
	if !mustFail(t, queue, job, "outage") {
		t.Fatal("fail returned false")
	}
	if _, found := mustClaim(t, queue, "export", "worker-1"); found {
		t.Fatal("retry ignored the backoff of a scheduled job")
	}
	time.Sleep(250 * time.Millisecond)
	retried := mustFoundClaim(t, queue, "export", "worker-1")
	if retried.ID != job.ID || retried.Attempts != 2 {
		t.Fatalf("retried job = %#v", retried)
	}
}

func TestConcurrentWorkersDeliverEachJobOnce(t *testing.T) {
	ctx := context.Background()
	queue := testQueue(t)
	const jobs = 20
	for range jobs {
		mustEnqueue(t, queue, "export", uuid.New())
	}
	var mu sync.Mutex
	delivered := make(map[int64]int, jobs)
	var wait sync.WaitGroup
	for workerIndex := range 8 {
		worker := mustWorker(t, pgoutbox.WorkerConfig{
			Queue: queue, Kinds: []string{"export"},
			WorkerID: fmt.Sprintf("worker-%d", workerIndex),
			Handler: func(_ context.Context, job pgoutbox.Job) error {
				mu.Lock()
				delivered[job.ID]++
				mu.Unlock()
				return nil
			},
		})
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				processed, err := worker.ProcessNext(ctx)
				if err != nil {
					t.Errorf("process next: %v", err)
					return
				}
				if !processed {
					return
				}
			}
		}()
	}
	wait.Wait()
	if len(delivered) != jobs {
		t.Fatalf("delivered %d of %d jobs", len(delivered), jobs)
	}
	for id, count := range delivered {
		if count != 1 {
			t.Fatalf("job %d was delivered %d times", id, count)
		}
	}
	mustStatsEqual(t, queue, nil, pgoutbox.Stats{})
}
