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

func TestNewWorkerValidatesConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewWorker(WorkerConfig{}); !errors.Is(err, ErrWorkerConfig) {
		t.Fatalf("NewWorker({}) error = %v", err)
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
		worker.jobTimeout != DefaultJobTimeout {
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
