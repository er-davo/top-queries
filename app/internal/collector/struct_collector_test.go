package collector

import (
	"testing"
	"time"

	"top-queries/internal/models"

	"github.com/stretchr/testify/assert"
)

func TestStructIndexedCollector_Lifecycle(t *testing.T) {
	cfg := Config{
		BucketDuration: 1 * time.Second,
		WindowDuration: 3 * time.Second,
	}
	c := NewStructIndexedCollector(cfg)
	now := time.Now()

	c.Add("iphone", now)
	c.Add("iphone", now.Add(500*time.Millisecond))
	c.Add("golang", now.Add(1*time.Second))
	c.Add("rust", now.Add(1*time.Second))

	flatData := c.Snapshot()
	topQueries := sortTop(flatData, 10)

	envelope := models.TopQueriesResponseEnvelope{
		Data:      topQueries,
		UpdatedAt: now.UTC(),
	}
	c.currentTop.Store(envelope)

	res1 := c.GetTopN(1)
	assert.Len(t, res1.Data, 1)
	assert.Equal(t, "iphone", res1.Data[0].Query)
	assert.Equal(t, int64(2), res1.Data[0].Count)

	res2 := c.GetTopN(2)
	assert.Len(t, res2.Data, 2)
	assert.Equal(t, "golang", res2.Data[1].Query)

	res3 := c.GetTopN(100)
	assert.Len(t, res3.Data, 3)
}
