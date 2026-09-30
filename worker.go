package pgoutbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

const (
	DefaultIdleInterval  = time.Second
	DefaultErrorInterval = 5 * time.Second
	DefaultJobTimeout    = time.Minute
)

// Handler processes one job. A nil error completes the job. An error retries
// the job with backoff unless it wraps ErrDrop, which dead-letters the job.
type Handler func(ctx context.Context, job Job) error

type WorkerConfig struct {
	Queue         *Queue
	Kinds         []string
	Handler       Handler
	WorkerID      string
	IdleInterval  time.Duration
	ErrorInterval time.Duration
	JobTimeout    time.Duration
	Logger        *slog.Logger
}

type Worker struct {
	queue         *Queue
	kinds         []string
	handler       Handler
	workerID      string
	idleInterval  time.Duration
	errorInterval time.Duration
	jobTimeout    time.Duration
	logger        *slog.Logger
}

// NewWorker validates the configuration and returns a worker. The intervals
// default to one second idle, five seconds after an error, and a one minute
// job timeout.
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.Queue == nil || len(config.Kinds) == 0 || config.Handler == nil ||
		strings.TrimSpace(config.WorkerID) == "" {
		return nil, ErrWorkerConfig
	}
	worker := &Worker{
		queue:         config.Queue,
		kinds:         append([]string(nil), config.Kinds...),
		handler:       config.Handler,
		workerID:      strings.TrimSpace(config.WorkerID),
		idleInterval:  config.IdleInterval,
		errorInterval: config.ErrorInterval,
		jobTimeout:    config.JobTimeout,
		logger:        config.Logger,
	}
	if worker.idleInterval <= 0 {
		worker.idleInterval = DefaultIdleInterval
	}
	if worker.errorInterval <= 0 {
		worker.errorInterval = DefaultErrorInterval
	}
	if worker.jobTimeout <= 0 {
		worker.jobTimeout = DefaultJobTimeout
	}
	if worker.logger == nil {
		worker.logger = slog.New(slog.DiscardHandler)
	}
	return worker, nil
}

// ProcessNext claims and handles one job. It returns false when no job is due.
func (worker *Worker) ProcessNext(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, ErrWorkerConfig
	}
	job, found, err := worker.queue.ClaimNext(ctx, worker.kinds, worker.workerID)
	if err != nil || !found {
		return false, err
	}
	handlerContext, cancel := context.WithTimeout(ctx, worker.jobTimeout)
	handlerErr := worker.handler(handlerContext, job)
	cancel()
	if handlerErr == nil {
		_, err = worker.queue.Complete(ctx, job)
		return true, err
	}
	reason := boundedReason(handlerErr.Error())
	if errors.Is(handlerErr, ErrDrop) {
		_, err = worker.queue.DeadLetter(ctx, job, reason)
		return true, err
	}
	_, err = worker.queue.Fail(ctx, job, reason)
	return true, err
}

// Run processes jobs until the context ends. It waits IdleInterval when no job
// is due and ErrorInterval after an error.
func (worker *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrWorkerConfig
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		processed, err := worker.ProcessNext(ctx)
		wait := worker.idleInterval
		switch {
		case err != nil:
			wait = worker.errorInterval
			if ctx.Err() == nil {
				worker.logger.ErrorContext(ctx, "outbox worker failed", "error", err)
			}
		case processed:
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}
