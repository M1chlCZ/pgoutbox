// Package prometheus adapts the pgoutbox Metrics interface to Prometheus
// collectors. The adapter lives in a separate package so the core pgoutbox
// package stays free of a Prometheus dependency.
//
// Register an adapter on a registry and pass it to WithMetrics:
//
//	adapter := outboxprometheus.New(prometheus.DefaultRegisterer, "pgoutbox")
//	queue, err := pgoutbox.New(pool, pgoutbox.WithMetrics(adapter))
package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/m1chlcz/pgoutbox"
)

// Adapter implements pgoutbox.Metrics with Prometheus collectors. Every
// collector carries a kind label.
type Adapter struct {
	published    *prometheus.CounterVec
	delivered    *prometheus.CounterVec
	failed       *prometheus.CounterVec
	retried      *prometheus.CounterVec
	deadLettered *prometheus.CounterVec
	pending      *prometheus.GaugeVec
}

var _ pgoutbox.Metrics = (*Adapter)(nil)

// New registers the collectors on the registerer and returns an adapter. A nil
// registerer uses prometheus.DefaultRegisterer. New panics when the registerer
// already has a collector with one of the metric names.
//
// With the namespace "pgoutbox" the adapter exposes:
//
//	pgoutbox_jobs_published_total{kind}
//	pgoutbox_jobs_delivered_total{kind}
//	pgoutbox_jobs_failed_total{kind}
//	pgoutbox_jobs_retried_total{kind}
//	pgoutbox_jobs_dead_lettered_total{kind}
//	pgoutbox_jobs_pending{kind}
func New(registerer prometheus.Registerer, namespace string) *Adapter {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	adapter := &Adapter{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_published_total",
			Help:      "Jobs published to the outbox by kind.",
		}, []string{"kind"}),
		delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_delivered_total",
			Help:      "Jobs completed by a worker by kind.",
		}, []string{"kind"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_failed_total",
			Help:      "Job attempts released for retry by kind.",
		}, []string{"kind"}),
		retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_retried_total",
			Help:      "Dead-lettered jobs returned to the queue by kind.",
		}, []string{"kind"}),
		deadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_dead_lettered_total",
			Help:      "Jobs moved to the dead-letter state by kind.",
		}, []string{"kind"}),
		pending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "jobs_pending",
			Help:      "Pending jobs by kind.",
		}, []string{"kind"}),
	}
	registerer.MustRegister(
		adapter.published,
		adapter.delivered,
		adapter.failed,
		adapter.retried,
		adapter.deadLettered,
		adapter.pending,
	)
	return adapter
}

// Published implements pgoutbox.Metrics.
func (adapter *Adapter) Published(kind string) {
	adapter.published.WithLabelValues(kind).Inc()
}

// Delivered implements pgoutbox.Metrics.
func (adapter *Adapter) Delivered(kind string) {
	adapter.delivered.WithLabelValues(kind).Inc()
}

// Failed implements pgoutbox.Metrics.
func (adapter *Adapter) Failed(kind string) {
	adapter.failed.WithLabelValues(kind).Inc()
}

// Retried implements pgoutbox.Metrics.
func (adapter *Adapter) Retried(kind string) {
	adapter.retried.WithLabelValues(kind).Inc()
}

// DeadLettered implements pgoutbox.Metrics.
func (adapter *Adapter) DeadLettered(kind string) {
	adapter.deadLettered.WithLabelValues(kind).Inc()
}

// Pending implements pgoutbox.Metrics.
func (adapter *Adapter) Pending(kind string, delta int64) {
	adapter.pending.WithLabelValues(kind).Add(float64(delta))
}
