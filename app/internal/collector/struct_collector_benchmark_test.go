package collector

import (
	"testing"
	"time"

	"top-queries/internal/models"
)

func BenchmarkStructIndexedCollector_Add(b *testing.B) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 5 * time.Minute,
	}
	c := NewStructIndexedCollector(cfg)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add("benchmark_query", now)
	}
}

// BenchmarkStructIndexedCollector_GetTopN evaluates the execution time and allocation profile
// of extracting and slicing a sub-segment from the pre-cached top queries dataset.
func BenchmarkStructIndexedCollector_GetTopN(b *testing.B) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 5 * time.Minute,
	}
	c := NewStructIndexedCollector(cfg)

	var items []models.TargetQuery
	for i := 0; i < 10; i++ {
		items = append(items, models.TargetQuery{Query: "query", Count: int64(100 - i)})
	}
	c.currentTop.Store(models.TopQueriesResponseEnvelope{
		Data:      items,
		UpdatedAt: time.Now().UTC(),
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetTopN(5)
	}
}
