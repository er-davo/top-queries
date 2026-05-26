package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// KafkaMessagesProcessed counts the total number of incoming search events read from Kafka.
	KafkaMessagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "top_queries_kafka_messages_total",
		Help: "The total number of processed Kafka messages",
	})

	// MessagesFiltered counts the numbers of dropped messages partitioned by the reason ("antifraud" or "stoplist").
	MessagesFiltered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "top_queries_filtered_messages_total",
		Help: "The total number of filtered messages",
	}, []string{"reason"})

	// HTTPRequestsTotal counts the total number of incoming HTTP requests to the /api/v1/top endpoint.
	HTTPRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "top_queries_http_requests_total",
		Help: "The total number of HTTP requests to /api/v1/top",
	})
)
