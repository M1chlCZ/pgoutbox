package prometheus_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/m1chlcz/pgoutbox"
	outboxprometheus "github.com/m1chlcz/pgoutbox/prometheus"
)

func TestAdapterGathersLifecycleMetrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	adapter := outboxprometheus.New(registry, "pgoutbox")
	var metrics pgoutbox.Metrics = adapter
	metrics.Published("export")
	metrics.Published("export")
	metrics.Delivered("export")
	metrics.Failed("export")
	metrics.Retried("export")
	metrics.DeadLettered("export")
	metrics.Pending("export", 3)
	metrics.Pending("export", -1)
	expected := strings.NewReader(`
# HELP pgoutbox_jobs_dead_lettered_total Jobs moved to the dead-letter state by kind.
# TYPE pgoutbox_jobs_dead_lettered_total counter
pgoutbox_jobs_dead_lettered_total{kind="export"} 1
# HELP pgoutbox_jobs_delivered_total Jobs completed by a worker by kind.
# TYPE pgoutbox_jobs_delivered_total counter
pgoutbox_jobs_delivered_total{kind="export"} 1
# HELP pgoutbox_jobs_failed_total Job attempts released for retry by kind.
# TYPE pgoutbox_jobs_failed_total counter
pgoutbox_jobs_failed_total{kind="export"} 1
# HELP pgoutbox_jobs_pending Pending jobs by kind.
# TYPE pgoutbox_jobs_pending gauge
pgoutbox_jobs_pending{kind="export"} 2
# HELP pgoutbox_jobs_published_total Jobs published to the outbox by kind.
# TYPE pgoutbox_jobs_published_total counter
pgoutbox_jobs_published_total{kind="export"} 2
# HELP pgoutbox_jobs_retried_total Dead-lettered jobs returned to the queue by kind.
# TYPE pgoutbox_jobs_retried_total counter
pgoutbox_jobs_retried_total{kind="export"} 1
`)
	names := []string{
		"pgoutbox_jobs_dead_lettered_total",
		"pgoutbox_jobs_delivered_total",
		"pgoutbox_jobs_failed_total",
		"pgoutbox_jobs_pending",
		"pgoutbox_jobs_published_total",
		"pgoutbox_jobs_retried_total",
	}
	if err := testutil.GatherAndCompare(registry, expected, names...); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	adapter := outboxprometheus.New(registry, "pgoutbox")
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				adapter.Published("export")
				adapter.Pending("export", 1)
			}
		}()
	}
	wait.Wait()
	expected := strings.NewReader(`
# HELP pgoutbox_jobs_pending Pending jobs by kind.
# TYPE pgoutbox_jobs_pending gauge
pgoutbox_jobs_pending{kind="export"} 800
# HELP pgoutbox_jobs_published_total Jobs published to the outbox by kind.
# TYPE pgoutbox_jobs_published_total counter
pgoutbox_jobs_published_total{kind="export"} 800
`)
	if err := testutil.GatherAndCompare(registry, expected,
		"pgoutbox_jobs_pending", "pgoutbox_jobs_published_total"); err != nil {
		t.Fatal(err)
	}
}

func TestNewWithoutNamespace(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	adapter := outboxprometheus.New(registry, "")
	adapter.Published("export")
	expected := strings.NewReader(`
# HELP jobs_published_total Jobs published to the outbox by kind.
# TYPE jobs_published_total counter
jobs_published_total{kind="export"} 1
`)
	if err := testutil.GatherAndCompare(registry, expected, "jobs_published_total"); err != nil {
		t.Fatal(err)
	}
}
