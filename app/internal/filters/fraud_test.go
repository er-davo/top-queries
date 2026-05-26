package filters

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"top-queries/internal/models"

	"github.com/stretchr/testify/require"
)

func TestAntiFraudFilter_Check_Limits(t *testing.T) {
	filter, err := NewAntiFraudFilter(100, 3, 2*time.Second)
	require.NoError(t, err)

	event := models.SearchEvent{
		IP:    "192.168.1.1",
		Query: "носки белые",
	}

	require.True(t, filter.Check(event))
	require.True(t, filter.Check(event))
	require.True(t, filter.Check(event))
	require.False(t, filter.Check(event))
}

func TestAntiFraudFilter_Check_Isolation_By_IP(t *testing.T) {
	filter, err := NewAntiFraudFilter(100, 1, 10*time.Second)
	require.NoError(t, err)

	event1 := models.SearchEvent{IP: "1.1.1.1", UserID: "user_1", Query: "iphone"}
	event2 := models.SearchEvent{IP: "2.2.2.2", UserID: "user_1", Query: "samsung"}
	event3 := models.SearchEvent{IP: "3.3.3.3", UserID: "user_2", Query: "iphone"}

	require.True(t, filter.Check(event1))
	require.True(t, filter.Check(event2))
	require.True(t, filter.Check(event3))

	repeatedEvent := models.SearchEvent{IP: "1.1.1.1", UserID: "user_bot", Query: "porshe"}
	require.False(t, filter.Check(repeatedEvent))
}

func TestAntiFraudFilter_Check_EmptyIP(t *testing.T) {
	filter, err := NewAntiFraudFilter(100, 1, 10*time.Second)
	require.NoError(t, err)

	event := models.SearchEvent{IP: "", Query: "iphone"}

	require.True(t, filter.Check(event))
	require.True(t, filter.Check(event))
}

func TestAntiFraudFilter_Check_Concurrency(t *testing.T) {
	filter, err := NewAntiFraudFilter(100, 500, 5*time.Second)
	require.NoError(t, err)

	event := models.SearchEvent{IP: "99.99.99.99", Query: "spam"}

	var wg sync.WaitGroup
	workers := 10
	requestsPerWorker := 100

	allowedCount := 0
	var mu sync.Mutex

	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for j := 0; j < requestsPerWorker; j++ {
				if filter.Check(event) {
					mu.Lock()
					allowedCount++
					mu.Unlock()
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	require.Equal(t, 500, allowedCount)
}

// BenchmarkAntiFraudFilter_Check_Parallel_SingleIP measures lock contention
// when concurrent goroutines hit the exact same IP address.
func BenchmarkAntiFraudFilter_Check_Parallel_SingleIP(b *testing.B) {
	filter, _ := NewAntiFraudFilter(10000, 10000000, 1*time.Minute)
	event := models.SearchEvent{
		IP:    "195.201.10.5",
		Query: "купить гитару fingerstyle",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = filter.Check(event)
		}
	})
}

// BenchmarkAntiFraudFilter_Check_Parallel_MultiIP evaluates LRU cache performance
// and cache eviction mechanics under highly distributed concurrent IP load.
func BenchmarkAntiFraudFilter_Check_Parallel_MultiIP(b *testing.B) {
	filter, _ := NewAntiFraudFilter(10000, 1000, 1*time.Minute)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		workerIP := fmt.Sprintf("172.20.%d.%d", rng.Intn(254)+1, rng.Intn(254)+1)

		event := models.SearchEvent{
			IP:    workerIP,
			Query: "платье летнее шелковое",
		}

		for pb.Next() {
			_ = filter.Check(event)
		}
	})
}

func BenchmarkAntiFraudFilter_Check(b *testing.B) {
	filter, _ := NewAntiFraudFilter(10000, 1000000, 1*time.Minute)
	event := models.SearchEvent{
		IP:    "172.20.10.5",
		Query: "высоконагруженные системы на Go",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filter.Check(event)
	}
}
