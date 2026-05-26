package collector

import (
	"sync/atomic"
	"time"

	"top-queries/internal/models"
)

// StructIndexedCollector manages a ring buffer of time-sliced buckets to capture
// search query frequencies concurrently. Unlike TimeIndexedCollector, it stores the
// pre-aggregated domain model structure instead of raw JSON inside its atomic value.
type StructIndexedCollector struct {
	cfg        Config
	numBuckets int
	bucketSec  int64
	buckets    []*bucket
	currentTop atomic.Value
}

// NewStructIndexedCollector initializes and returns a StructIndexedCollector instance.
func NewStructIndexedCollector(cfg Config) *StructIndexedCollector {
	numBuckets := int(cfg.WindowDuration / cfg.BucketDuration)
	buckets := make([]*bucket, numBuckets)

	for i := 0; i < numBuckets; i++ {
		buckets[i] = &bucket{counters: make(map[string]int64)}
	}

	c := &StructIndexedCollector{
		cfg:        cfg,
		numBuckets: numBuckets,
		bucketSec:  int64(cfg.BucketDuration.Seconds()),
		buckets:    buckets,
	}

	c.currentTop.Store(models.TopQueriesResponseEnvelope{
		Data:      []models.TargetQuery{},
		UpdatedAt: time.Now().UTC(),
	})

	return c
}

// Add increments the frequency counter of a search query in a bucket corresponding to the given timestamp.
func (c *StructIndexedCollector) Add(query string, timestamp time.Time) {
	ts := timestamp.Unix()
	bucketIndex := (ts / c.bucketSec) % int64(c.numBuckets)
	bucket := c.buckets[bucketIndex]
	expectedExpiresAt := (ts / c.bucketSec) * c.bucketSec

	bucket.mu.Lock()
	if bucket.expiresAt < expectedExpiresAt {
		for k := range bucket.counters {
			delete(bucket.counters, k)
		}
		bucket.expiresAt = expectedExpiresAt
	}
	bucket.counters[query]++
	bucket.mu.Unlock()
}

// Snapshot aggregates the query frequency map across all active and non-expired buckets.
func (c *StructIndexedCollector) Snapshot() map[string]int64 {
	now := time.Now().Unix()
	oldestValidTime := now - int64(c.cfg.WindowDuration.Seconds())
	result := make(map[string]int64)

	for _, bucket := range c.buckets {
		bucket.mu.RLock()
		if bucket.expiresAt >= oldestValidTime {
			for query, count := range bucket.counters {
				result[query] += count
			}
		}
		bucket.mu.RUnlock()
	}
	return result
}

// GetTopN fetches the pre-aggregated envelope atomically and returns a capped shallow copy
// up to the specified limit parameter.
func (c *StructIndexedCollector) GetTopN(limit int) models.TopQueriesResponseEnvelope {
	cached := c.currentTop.Load().(models.TopQueriesResponseEnvelope)

	if limit <= 0 || limit >= len(cached.Data) {
		return cached
	}

	return models.TopQueriesResponseEnvelope{
		Data:      cached.Data[:limit],
		UpdatedAt: cached.UpdatedAt,
	}
}

// StartAggregatorWorker spins up a background processing routine that periodically evaluates
// active metrics snapshots, computes top queries, and flushes the structure to the atomic cache.
func (c *StructIndexedCollector) StartAggregatorWorker(tickerDuration time.Duration, maxTopLimit int) {
	go func() {
		ticker := time.NewTicker(tickerDuration)
		defer ticker.Stop()

		for range ticker.C {
			flatData := c.Snapshot()
			topQueries := sortTop(flatData, maxTopLimit)

			envelope := models.TopQueriesResponseEnvelope{
				Data:      topQueries,
				UpdatedAt: time.Now().UTC(),
			}
			c.currentTop.Store(envelope)
		}
	}()
}
