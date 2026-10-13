//nolint:testpackage // White-box tests exercise unexported validation helpers, Queue fields and defaults.
package pgoutbox

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); !errors.Is(err, ErrPoolRequired) {
		t.Fatalf("New(nil) error = %v", err)
	}
}

func TestOptionsValidate(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	for _, option := range []Option{
		WithMaxAttempts(0),
		WithMaxAttempts(1001),
		WithLease(0),
		WithBackoff(Backoff{}),
	} {
		if err := option(queue); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("option error = %v, want invalid input", err)
		}
	}
	if err := WithOrderedAggregates(true)(queue); err != nil || !queue.ordered {
		t.Fatalf("WithOrderedAggregates error = %v", err)
	}
}

type testMetrics struct {
	NoopMetrics
}

func TestWithMetricsOption(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	if err := WithMetrics(nil)(queue); err != nil || queue.metrics != (NoopMetrics{}) {
		t.Fatalf("WithMetrics(nil) = %v, metrics %#v", err, queue.metrics)
	}
	metrics := &testMetrics{}
	if err := WithMetrics(metrics)(queue); err != nil || queue.metrics != metrics {
		t.Fatalf("WithMetrics(custom) = %v, metrics %#v", err, queue.metrics)
	}
}

func TestEnqueueRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	for _, input := range []EnqueueInput{
		{},
		{Kind: "export", AggregateType: "widget", Payload: []byte(`{}`)},
		{Kind: "export", AggregateType: "widget", AggregateID: uuid.Nil, Payload: []byte(`[]`)},
	} {
		if _, err := queue.Enqueue(context.Background(), input); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Enqueue(%+v) error = %v", input, err)
		}
	}
}

func TestEnqueueBatchRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	ctx := context.Background()
	valid := []EnqueueInput{{Kind: "export", AggregateType: "widget", AggregateID: uuid.New(), Payload: []byte(`{}`)}}
	if _, err := queue.EnqueueBatch(nil, valid); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("EnqueueBatch with a nil context error = %v", err)
	}
	if _, err := queue.EnqueueBatch(ctx, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("EnqueueBatch with no jobs error = %v", err)
	}
	if _, err := queue.EnqueueBatch(ctx, make([]EnqueueInput, MaxBatchSize+1)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("EnqueueBatch above the limit error = %v", err)
	}
	invalid := []EnqueueInput{valid[0], {Kind: " ", AggregateType: "widget", AggregateID: uuid.New(), Payload: []byte(`{}`)}}
	if _, err := queue.EnqueueBatch(ctx, invalid); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("EnqueueBatch with an invalid job error = %v", err)
	}
}

func TestClaimRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	if _, _, err := queue.ClaimNext(context.Background(), nil, "worker"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimNext with no kinds error = %v", err)
	}
	if _, _, err := queue.ClaimNext(context.Background(), []string{"export"}, " "); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimNext with a blank worker error = %v", err)
	}
}

func TestClaimBatchRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	queue := &Queue{}
	ctx := context.Background()
	if _, err := queue.ClaimBatch(nil, []string{"export"}, "worker", 1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimBatch with a nil context error = %v", err)
	}
	if _, err := queue.ClaimBatch(ctx, nil, "worker", 1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimBatch with no kinds error = %v", err)
	}
	if _, err := queue.ClaimBatch(ctx, []string{"export"}, " ", 1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimBatch with a blank worker error = %v", err)
	}
	if _, err := queue.ClaimBatch(ctx, []string{"export"}, "worker", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ClaimBatch with a zero limit error = %v", err)
	}
	if _, err := queue.ClaimBatch(ctx, []string{"export"}, "worker", MaxBatchSize+1); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("ClaimBatch above the limit error = %v", err)
	}
}

func TestNewWorkerValidatesConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewWorker(WorkerConfig{}); !errors.Is(err, ErrWorkerConfig) {
		t.Fatalf("NewWorker({}) error = %v", err)
	}
	if _, err := NewWorker(WorkerConfig{
		Queue: &Queue{}, Kinds: []string{"export"}, Handler: func(context.Context, Job) error { return nil },
		WorkerID: "worker-1", Concurrency: -1,
	}); !errors.Is(err, ErrWorkerConfig) {
		t.Fatalf("NewWorker with a negative concurrency error = %v", err)
	}
	for _, size := range []int{-1, MaxBatchSize + 1} {
		if _, err := NewWorker(WorkerConfig{
			Queue: &Queue{}, Kinds: []string{"export"}, Handler: func(context.Context, Job) error { return nil },
			WorkerID: "worker-1", ClaimBatchSize: size,
		}); !errors.Is(err, ErrWorkerConfig) {
			t.Fatalf("NewWorker with claim batch size %d error = %v", size, err)
		}
	}
	worker, err := NewWorker(WorkerConfig{
		Queue:    &Queue{},
		Kinds:    []string{"export"},
		Handler:  func(context.Context, Job) error { return nil },
		WorkerID: "worker-1",
	})
	if err != nil {
		t.Fatalf("NewWorker error = %v", err)
	}
	if worker.idleInterval != DefaultIdleInterval || worker.errorInterval != DefaultErrorInterval ||
		worker.jobTimeout != DefaultJobTimeout || worker.concurrency != 1 || worker.claimBatchSize != 0 {
		t.Fatalf("worker defaults = %+v", worker)
	}
}

func TestBoundedReasonTruncates(t *testing.T) {
	t.Parallel()
	reason := string(make([]byte, maxReasonLength+10))
	if len(boundedReason(reason)) != maxReasonLength {
		t.Fatalf("bounded reason length = %d", len(boundedReason(reason)))
	}
	if boundedReason("short") != "short" {
		t.Fatalf("bounded reason changed a short value")
	}
	if !validPayload([]byte(`{"a":1}`)) || validPayload([]byte(`[1]`)) || validPayload(nil) {
		t.Fatal("payload validation is incorrect")
	}
	if !validClaim(Job{ID: 1, Attempts: 1, WorkerID: "w"}) ||
		validClaim(Job{ID: 0, Attempts: 1, WorkerID: "w"}) {
		t.Fatal("claim validation is incorrect")
	}
}
