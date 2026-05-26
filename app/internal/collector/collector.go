package collector

import (
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"top-queries/internal/models"
)

type bucket struct {
	mu        sync.RWMutex
	expiresAt int64
	counters  map[string]int64
}

// Config wraps operational parameters for the sliding window analytics.
type Config struct {
	BucketDuration time.Duration
	WindowDuration time.Duration
}

// TimeIndexedCollector manages a ring buffer of time-sliced buckets to capture
// and aggregate search query frequencies concurrently. It provides lock-free
// reads for pre-rendered JSON responses via atomic storage.
type TimeIndexedCollector struct {
	cfg        Config
	numBuckets int
	bucketSec  int64
	buckets    []*bucket
	currentTop atomic.Value
}

// NewTimeIndexedCollector initializes and returns a TimeIndexedCollector pre-allocating the ring buffer buckets.
func NewTimeIndexedCollector(cfg Config) *TimeIndexedCollector {
	numBuckets := int(cfg.WindowDuration / cfg.BucketDuration)
	buckets := make([]*bucket, numBuckets)

	for i := 0; i < numBuckets; i++ {
		buckets[i] = &bucket{
			counters: make(map[string]int64),
		}
	}

	c := &TimeIndexedCollector{
		cfg:        cfg,
		numBuckets: numBuckets,
		bucketSec:  int64(cfg.BucketDuration.Seconds()),
		buckets:    buckets,
	}

	defaultResponse := models.TopQueriesResponseEnvelope{
		Data:      []models.TargetQuery{},
		UpdatedAt: time.Now().UTC(),
	}
	defaultBytes, _ := json.Marshal(defaultResponse)
	c.currentTop.Store(defaultBytes)

	return c
}

// Add increments the frequency counter of a search query in a bucket corresponding to the given timestamp.
func (c *TimeIndexedCollector) Add(query string, timestamp time.Time) {
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
func (c *TimeIndexedCollector) Snapshot() map[string]int64 {
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

// GetTopJSON retrieves the latest pre-rendered JSON payload atomically without blocking incoming writes.
func (c *TimeIndexedCollector) GetTopJSON() []byte {
	return c.currentTop.Load().([]byte)
}

// StartAggregatorWorker spins up a background processing routine that periodically evaluates
// active metrics snapshots, computes top queries, and flushes them to the atomic cache.
func (c *TimeIndexedCollector) StartAggregatorWorker(tickerDuration time.Duration, topLimit int) {
	go func() {
		ticker := time.NewTicker(tickerDuration)
		defer ticker.Stop()

		for range ticker.C {
			flatData := c.Snapshot()
			topQueries := sortTop(flatData, topLimit)

			envelope := models.TopQueriesResponseEnvelope{
				Data:      topQueries,
				UpdatedAt: time.Now().UTC(),
			}

			jsonBytes, err := json.Marshal(envelope)
			if err != nil {
				continue
			}

			c.currentTop.Store(jsonBytes)
		}
	}()
}

func sortTop(data map[string]int64, limit int) []models.TargetQuery {
	if len(data) == 0 {
		return []models.TargetQuery{}
	}

	list := make([]models.TargetQuery, 0, len(data))
	for query, count := range data {
		list = append(list, models.TargetQuery{Query: query, Count: count})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Count == list[j].Count {
			return list[i].Query < list[j].Query
		}
		return list[i].Count > list[j].Count
	})

	if len(list) > limit {
		list = list[:limit]
	}

	return list
}
