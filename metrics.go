package pgoutbox

// Metrics receives lifecycle counters and a pending gauge from the queue.
//
// The queue calls every method synchronously from the goroutine that performed
// the database change, after the statement reports the expected row change. A
// transition that changes no row, such as an enqueue conflict or a fenced
// stale attempt, fires no callback. Implementations must be safe for
// concurrent use, must not block, and must not call back into the queue. A
// panic in a method propagates to the caller.
//
// The callbacks fire as follows:
//
//   - Published: Enqueue or one position of EnqueueBatch inserted a new job.
//   - Delivered: Complete marked a claimed job processed.
//   - Failed: Fail returned a claimed job to pending with a backoff delay.
//     A failure at the attempt limit calls DeadLettered instead.
//   - Retried: Redeliver or Retry returned a dead-lettered job to pending.
//   - DeadLettered: a claimed job entered the dead-letter state, through
//     DeadLetter, through Fail at the attempt limit, or through a worker
//     error that wraps ErrDrop.
//   - Pending: the pending count changed. The delta is +1 on publish, on a
//     failed attempt with a retry, and on redelivery. The delta is -1 on a
//     claim, through ClaimNext or each job of ClaimBatch, and on dropping a
//     pending job. Completion and dead-lettering fire no Pending callback
//     because the job left the pending count when it was claimed.
//
// A failed attempt calls Failed before Pending. Redelivery calls Retried
// before Pending. A lease that expires without a later Complete, Fail, or
// DeadLetter call is not observed; callers that need an exact gauge must
// reconcile with Stats.
type Metrics interface {
	Published(kind string)
	Delivered(kind string)
	Failed(kind string)
	Retried(kind string)
	DeadLettered(kind string)
	Pending(kind string, delta int64)
}

// NoopMetrics does nothing. It is the default hook; embed it to override only
// the methods you need.
type NoopMetrics struct{}

// Published implements Metrics.
func (NoopMetrics) Published(string) {}

// Delivered implements Metrics.
func (NoopMetrics) Delivered(string) {}

// Failed implements Metrics.
func (NoopMetrics) Failed(string) {}

// Retried implements Metrics.
func (NoopMetrics) Retried(string) {}

// DeadLettered implements Metrics.
func (NoopMetrics) DeadLettered(string) {}

// Pending implements Metrics.
func (NoopMetrics) Pending(string, int64) {}
