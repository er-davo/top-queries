package filters

import (
	"testing"

	"top-queries/internal/models"
)

var (
	shortQuery = "купить вейп"
	longQuery  = "привет, я хочу купить красивую белую футболку мужскую без принтера и еще вейп"
)

func BenchmarkStopListFilter_Check(b *testing.B) {
	f, _ := NewStopListFilter([]string{"скам", "фрод", "вейп", "казино", "ставки"})
	eventShort := models.SearchEvent{Query: shortQuery}
	eventLong := models.SearchEvent{Query: longQuery}

	b.ResetTimer()

	b.Run("ShortQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = f.Check(eventShort)
		}
	})

	b.Run("LongQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = f.Check(eventLong)
		}
	})
}

// BenchmarkStopListFilter_Check_Parallel evaluates the concurrent read performance
// and lock-free execution of the Aho-Corasick filter under heavy multi-threaded load.
func BenchmarkStopListFilter_Check_Parallel(b *testing.B) {
	f, _ := NewStopListFilter([]string{"скам", "фрод", "вейп", "казино", "ставки"})
	event := models.SearchEvent{Query: longQuery}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = f.Check(event)
		}
	})
}
