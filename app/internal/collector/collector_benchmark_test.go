package collector

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkTimeIndexedCollector_Add(b *testing.B) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 5 * time.Minute,
	}
	c := NewTimeIndexedCollector(cfg)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add("поисковый запрос", now)
	}
}

// BenchmarkTimeIndexedCollector_Add_Parallel evaluates mutex contention and write throughput
// of the collector when simulated concurrent consumer workers append events into underlying buckets.
func BenchmarkTimeIndexedCollector_Add_Parallel(b *testing.B) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 5 * time.Minute,
	}
	c := NewTimeIndexedCollector(cfg)
	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add("конкурентный запрос", now)
		}
	})
}

// BenchmarkTimeIndexedCollector_Aggregation evaluates the cost of dynamic sliding-window slicing,
// data flattening, and subsequent quicksort operations performed periodically by background routines.
func BenchmarkTimeIndexedCollector_Aggregation(b *testing.B) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 5 * time.Minute,
	}
	c := NewTimeIndexedCollector(cfg)
	now := time.Now()

	for i := 0; i < 1000; i++ {
		query := fmt.Sprintf("query_%d", i)
		tShift := now.Add(-time.Duration(i) * time.Second)
		c.Add(query, tShift)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		flatData := c.Snapshot()
		_ = sortTop(flatData, 10)
	}
}
