package pgoutbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
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
	Queue          *Queue
	Kinds          []string
	Handler        Handler
	WorkerID       string
	IdleInterval   time.Duration
	ErrorInterval  time.Duration
	JobTimeout     time.Duration
	Concurrency    int
	ClaimBatchSize int
	Logger         *slog.Logger
}

type Worker struct {
	queue          *Queue
	kinds          []string
	handler        Handler
	workerID       string
	idleInterval   time.Duration
	errorInterval  time.Duration
	jobTimeout     time.Duration
	concurrency    int
	claimBatchSize int
	logger         *slog.Logger
}

// NewWorker validates the configuration and returns a worker. The intervals
// default to one second idle, five seconds after an error, and a one minute
// job timeout. Concurrency defaults to one claim loop, and a zero
// ClaimBatchSize keeps single-job claims.
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.Queue == nil || len(config.Kinds) == 0 || config.Handler == nil ||
		strings.TrimSpace(config.WorkerID) == "" {
		return nil, ErrWorkerConfig
	}
	if config.Concurrency < 0 || config.ClaimBatchSize < 0 || config.ClaimBatchSize > MaxBatchSize {
		return nil, ErrWorkerConfig
	}
	worker := &Worker{
		queue:          config.Queue,
		kinds:          append([]string(nil), config.Kinds...),
		handler:        config.Handler,
		workerID:       strings.TrimSpace(config.WorkerID),
		idleInterval:   config.IdleInterval,
		errorInterval:  config.ErrorInterval,
		jobTimeout:     config.JobTimeout,
		concurrency:    config.Concurrency,
		claimBatchSize: config.ClaimBatchSize,
		logger:         config.Logger,
	}
	if worker.concurrency == 0 {
		worker.concurrency = 1
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
// ClaimBatchSize does not change this single-job behavior.
func (worker *Worker) ProcessNext(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, ErrWorkerConfig
	}
	job, found, err := worker.queue.ClaimNext(ctx, worker.kinds, worker.workerID)
	if err != nil || !found {
		return false, err
	}
	return true, worker.handle(ctx, job)
}

// Run processes jobs until the context ends. It starts Concurrency claim
// loops and returns after every loop has stopped; a loop that is inside a
// handler finishes the handler first. A claim batch is never used with
// ordered aggregates.
func (worker *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrWorkerConfig
	}
	var wait sync.WaitGroup
	for range worker.concurrency {
		wait.Add(1)
		go func() {
			defer wait.Done()
			worker.runLoop(ctx)
		}()
	}
	wait.Wait()
	return nil
}

func (worker *Worker) runLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		processed, err := worker.processNextBatch(ctx)
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
			return
		case <-timer.C:
		}
	}
}

func (worker *Worker) processNextBatch(ctx context.Context) (bool, error) {
	if worker.claimBatchSize > 1 && !worker.queue.ordered {
		jobs, err := worker.queue.ClaimBatch(ctx, worker.kinds, worker.workerID, worker.claimBatchSize)
		if err != nil || len(jobs) == 0 {
			return false, err
		}
		var processErr error
		for _, job := range jobs {
			if err := worker.handle(ctx, job); err != nil && processErr == nil {
				processErr = err
			}
		}
		return true, processErr
	}
	return worker.ProcessNext(ctx)
}

func (worker *Worker) handle(ctx context.Context, job Job) error {
	handlerContext, cancel := context.WithTimeout(ctx, worker.jobTimeout)
	handlerErr := worker.handler(handlerContext, job)
	cancel()
	if handlerErr == nil {
		_, err := worker.queue.Complete(ctx, job)
		return err
	}
	reason := boundedReason(handlerErr.Error())
	if errors.Is(handlerErr, ErrDrop) {
		_, err := worker.queue.DeadLetter(ctx, job, reason)
		return err
	}
	_, err := worker.queue.Fail(ctx, job, reason)
	return err
}
