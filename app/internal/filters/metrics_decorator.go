package filters

import (
	"top-queries/internal/metrics"
	"top-queries/internal/models"
)

// MetricsDecorator wraps an underlying Filter and increments Prometheus counters
// with a specific reason label whenever a validation check fails.
type MetricsDecorator struct {
	inner  Filter
	reason string
}

// NewMetricsDecorator initializes and returns a new metrics-instrumented Filter decorator.
func NewMetricsDecorator(inner Filter, reason string) Filter {
	return &MetricsDecorator{
		inner:  inner,
		reason: reason,
	}
}

// Check executes the underlying filter logic and increments metric counters on failure.
func (d *MetricsDecorator) Check(event models.SearchEvent) bool {
	ok := d.inner.Check(event)
	if !ok {
		metrics.MessagesFiltered.WithLabelValues(d.reason).Inc()
	}
	return ok
}
