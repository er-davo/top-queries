package collector

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeIndexedCollector_AddAndSnapshot(t *testing.T) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 15 * time.Second,
	}
	c := NewTimeIndexedCollector(cfg)

	baseTime := time.Now()

	c.Add("go", baseTime.Add(-1*time.Second))
	c.Add("go", baseTime.Add(-2*time.Second))
	c.Add("kafka", baseTime.Add(-2*time.Second))
	c.Add("redis", baseTime.Add(-6*time.Second))

	snap := c.Snapshot()

	assert.Equal(t, int64(2), snap["go"])
	assert.Equal(t, int64(1), snap["kafka"])
	assert.Equal(t, int64(1), snap["redis"])
}

func TestTimeIndexedCollector_BucketExpiry(t *testing.T) {
	cfg := Config{
		BucketDuration: 5 * time.Second,
		WindowDuration: 10 * time.Second,
	}
	c := NewTimeIndexedCollector(cfg)

	nowUnix := time.Now().Unix()
	baseTime := time.Unix((nowUnix/5)*5, 0)

	c.Add("old_spam", baseTime)

	futureTime := baseTime.Add(10 * time.Second)
	c.Add("new_query", futureTime)

	snap := c.Snapshot()

	_, exists := snap["old_spam"]
	assert.False(t, exists)
	assert.Equal(t, int64(1), snap["new_query"])
}

func TestSortTop(t *testing.T) {
	data := map[string]int64{
		"kafka": 5,
		"go":    10,
		"redis": 3,
		"zero":  0,
	}

	top := sortTop(data, 5)
	require.Len(t, top, 4)
	assert.Equal(t, "go", top[0].Query)
	assert.Equal(t, int64(10), top[0].Count)

	topLimited := sortTop(data, 2)
	require.Len(t, topLimited, 2)
	assert.Equal(t, "go", topLimited[0].Query)
	assert.Equal(t, "kafka", topLimited[1].Query)
}

func TestTimeIndexedCollector_Concurrency(t *testing.T) {
	cfg := Config{
		BucketDuration: 1 * time.Second,
		WindowDuration: 5 * time.Second,
	}
	c := NewTimeIndexedCollector(cfg)

	var wg sync.WaitGroup
	workers := 8
	iterations := 1000

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			now := time.Now()
			for j := 0; j < iterations; j++ {
				c.Add("concurrent_query", now)
			}
		}()
	}

	stopReader := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = c.Snapshot()
			}
		}
	}()

	wg.Wait()
	close(stopReader)

	snap := c.Snapshot()
	assert.Equal(t, int64(workers*iterations), snap["concurrent_query"])
}
