package broker

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// WaitKafkaConsumersGroupReadiness blocks until the specified Kafka broker is reachable
// and metadata for the given topics can be successfully retrieved.
// It panics if the readiness condition is not met within a hardcoded 2-minute timeout.
func WaitKafkaConsumersGroupReadiness(brokerAddress string, topics ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			panic("Kafka not ready in time")
		default:
			conn, err := kafka.Dial("tcp", brokerAddress)
			if err == nil {
				_, err = conn.ReadPartitions(topics...)
				conn.Close()
				if err == nil {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}
