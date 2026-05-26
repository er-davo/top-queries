package filters

import (
	"sync"
	"time"

	"top-queries/internal/models"

	lru "github.com/hashicorp/golang-lru/v2"
)

// AntiFraudFilter implements a rate-limiting filter based on an LRU cache
// to track request frequencies per IP address.
type AntiFraudFilter struct {
	cache *lru.Cache[string, *RequestCounter]
	limit int64
	ttl   time.Duration
}

// RequestCounter keeps track of requests hit count and timing for a single IP address.
type RequestCounter struct {
	mu        sync.Mutex
	count     int64
	updatedAt time.Time
}

// NewAntiFraudFilter initializes and returns a new AntiFraudFilter instance.
func NewAntiFraudFilter(size int, limit int64, ttl time.Duration) (*AntiFraudFilter, error) {
	cache, err := lru.New[string, *RequestCounter](size)
	if err != nil {
		return nil, err
	}
	return &AntiFraudFilter{
		cache: cache,
		limit: limit,
		ttl:   ttl,
	}, nil
}

// Check evaluates whether the incoming event complies with the configured rate limits.
func (f *AntiFraudFilter) Check(event models.SearchEvent) bool {
	if event.IP == "" {
		return true
	}

	key := event.IP
	now := time.Now()

	val, exists := f.cache.Get(key)
	if !exists {
		counter := &RequestCounter{
			count:     1,
			updatedAt: now,
		}
		f.cache.Add(key, counter)
		return true
	}

	val.mu.Lock()
	defer val.mu.Unlock()

	if now.Sub(val.updatedAt) > f.ttl {
		val.count = 1
		val.updatedAt = now
		return true
	}

	val.count++

	return val.count <= f.limit
}
