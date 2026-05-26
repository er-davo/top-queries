package consumer

import (
	"context"
	"encoding/json"
	"time"

	"top-queries/internal/filters"
	"top-queries/internal/metrics"
	"top-queries/internal/models"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Collector defines the interface for aggregating search event frequencies.
type Collector interface {
	Add(query string, timestamp time.Time)
}

// SearchLogConsumer manages the lifecycle of reading and processing search log events from Apache Kafka.
type SearchLogConsumer struct {
	consumer  *kafka.Reader
	collector Collector
	filter    filters.Filter
	log       *zap.Logger
}

// NewSearchLogConsumer initializes and returns a new SearchLogConsumer instance.
func NewSearchLogConsumer(
	consumer *kafka.Reader,
	collector Collector,
	filter filters.Filter,
	log *zap.Logger,
) *SearchLogConsumer {
	return &SearchLogConsumer{
		consumer:  consumer,
		collector: collector,
		filter:    filter,
		log:       log,
	}
}

// Start runs an event loop that reads messages from Kafka, evaluates incoming data against
// filters, updates collector records, and increments processing metrics.
func (c *SearchLogConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.consumer.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.log.Error("failed to read message from kafka", zap.Error(err))
				return err
			}

			metrics.KafkaMessagesProcessed.Inc()

			var event models.SearchEvent
			err = json.Unmarshal(msg.Value, &event)
			if err != nil {
				c.log.Error("failed to unmarshal search event", zap.Binary("payload", msg.Value), zap.Error(err))
				continue
			}

			if ok := c.filter.Check(event); !ok {
				continue
			}

			c.collector.Add(event.Query, time.Unix(event.Timestamp, 0))
		}
	}
}

// Close gracefully stops the underlying Kafka connection reader.
func (c *SearchLogConsumer) Close() error {
	return c.consumer.Close()
}
