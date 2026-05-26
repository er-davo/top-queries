package models

import "time"

// SearchEvent represents an incoming search log event from the message broker.
type SearchEvent struct {
	Query     string `json:"query"`
	UserID    string `json:"user_id"`
	IP        string `json:"ip"`
	Timestamp int64  `json:"timestamp"`
}

// TargetQuery represents a single popular query inside the top list.
type TargetQuery struct {
	Query string `json:"query"`
	Count int64  `json:"count"`
}

// TopQueriesResponseEnvelope defines the API response structure compliant with OpenAPI spec.
type TopQueriesResponseEnvelope struct {
	Data      []TargetQuery `json:"data"`
	UpdatedAt time.Time     `json:"updated_at"`
}
