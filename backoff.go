package pgoutbox

import "time"

const (
	defaultBackoffBase     = 30 * time.Second
	defaultBackoffMax      = 30 * time.Minute
	defaultBackoffShiftMax = 6
)

// Backoff describes the retry delay: base << (attempts - 1), capped at Max,
// with an optional per-job jitter below one base step.
type Backoff struct {
	Base     time.Duration
	Max      time.Duration
	ShiftMax int
	Jitter   bool
}

// DefaultBackoff doubles from 30 seconds to a cap of 30 minutes.
var DefaultBackoff = Backoff{ //nolint:gochecknoglobals // Exported default configuration value.
	Base:     defaultBackoffBase,
	Max:      defaultBackoffMax,
	ShiftMax: defaultBackoffShiftMax,
	Jitter:   true,
}

func (backoff Backoff) validate() error {
	if backoff.Base <= 0 || backoff.Max < backoff.Base || backoff.Max > 24*time.Hour ||
		backoff.ShiftMax < 0 || backoff.ShiftMax > 30 {
		return ErrInvalidInput
	}
	return nil
}

// Delay returns the retry delay for an attempt count and a job ID.
func (backoff Backoff) Delay(attempts int, jobID int64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if jobID < 0 {
		jobID = -jobID
	}
	delay := min(backoff.Base<<min(attempts-1, backoff.ShiftMax), backoff.Max)
	if backoff.Jitter && backoff.Base >= time.Second {
		delay += time.Duration(jobID%int64(backoff.Base/time.Second)) * time.Second
	}
	return delay
}
