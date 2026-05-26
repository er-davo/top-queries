package filters

import "top-queries/internal/models"

// Filter defines a unified interface for validating incoming search events.
// Check returns true if the event passes validation, and false if it should be dropped.
type Filter interface {
	Check(event models.SearchEvent) bool
}

type chain struct {
	filters []Filter
}

// Check evaluates the search event against all filters in the chain sequentially.
// It short-circuits and returns false immediately if any filter fails.
func (c *chain) Check(event models.SearchEvent) bool {
	for _, filter := range c.filters {
		if ok := filter.Check(event); !ok {
			return false
		}
	}
	return true
}

// NewChain creates a composite filter that executes the provided filters in sequence.
func NewChain(filters ...Filter) Filter {
	return &chain{filters: filters}
}
