//nolint:testpackage // White-box test exercises the unexported Backoff.validate helper.
package pgoutbox

import (
	"testing"
	"time"
)

func TestDelayDoublesAndCaps(t *testing.T) {
	t.Parallel()
	backoff := Backoff{Base: time.Second, Max: 8 * time.Second, ShiftMax: 10}
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 8 * time.Second, 8 * time.Second,
	}
	for index, expected := range want {
		if got := backoff.Delay(index+1, 0); got != expected {
			t.Fatalf("Delay(%d) = %v, want %v", index+1, got, expected)
		}
	}
}

func TestDelayAddsJobJitter(t *testing.T) {
	t.Parallel()
	backoff := Backoff{Base: 10 * time.Second, Max: time.Minute, ShiftMax: 0, Jitter: true}
	if got := backoff.Delay(1, 0); got != 10*time.Second {
		t.Fatalf("Delay(1, 0) = %v", got)
	}
	if got := backoff.Delay(1, 3); got != 13*time.Second {
		t.Fatalf("Delay(1, 3) = %v", got)
	}
	noJitter := Backoff{Base: 10 * time.Second, Max: time.Minute, ShiftMax: 0}
	if got := noJitter.Delay(1, 3); got != 10*time.Second {
		t.Fatalf("Delay(1, 3) without jitter = %v", got)
	}
}

func TestBackoffValidation(t *testing.T) {
	t.Parallel()
	for _, backoff := range []Backoff{
		{},
		{Base: time.Second, Max: time.Second, ShiftMax: 31},
		{Base: time.Minute, Max: time.Second},
	} {
		if err := backoff.validate(); err == nil {
			t.Fatalf("validate(%+v) accepted an invalid backoff", backoff)
		}
	}
}
