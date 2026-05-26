package filters

import (
	"sync"
	"testing"

	"top-queries/internal/models"

	"github.com/stretchr/testify/require"
)

func TestStopListFilter_Check(t *testing.T) {
	tests := []struct {
		name         string
		initialWords []string
		query        string
		wantValid    bool
	}{
		{
			name:         "empty filter allows all",
			initialWords: []string{},
			query:        "купить футболку",
			wantValid:    true,
		},
		{
			name:         "clean query passed",
			initialWords: []string{"скам", "фрод", "вейп"},
			query:        "белые кроссовки nike",
			wantValid:    true,
		},
		{
			name:         "query contains exact stop word",
			initialWords: []string{"скам", "фрод", "вейп"},
			query:        "где купить вейп недорого",
			wantValid:    false,
		},
		{
			name:         "case insensitivity",
			initialWords: []string{"СкАм"},
			query:        "Новый СКАМ в телеграм",
			wantValid:    false,
		},
		{
			name:         "spaces trimming on init",
			initialWords: []string{"  фрод  "},
			query:        "проверка на фрод системы",
			wantValid:    false,
		},
		{
			name:         "substring matching",
			initialWords: []string{"вейп"},
			query:        "вейперы заполонили город",
			wantValid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewStopListFilter(tt.initialWords)
			require.NoError(t, err)

			event := models.SearchEvent{Query: tt.query}
			got := f.Check(event)

			require.Equal(t, tt.wantValid, got)
		})
	}
}

func TestStopListFilter_Add(t *testing.T) {
	f, err := NewStopListFilter([]string{"скам"})
	require.NoError(t, err)

	require.True(t, f.Check(models.SearchEvent{Query: "купить вейп"}))

	err = f.Add([]string{"вейп", "скам", "   ", "ФРОД"})
	require.NoError(t, err)

	require.False(t, f.Check(models.SearchEvent{Query: "купить вейп"}))
	require.False(t, f.Check(models.SearchEvent{Query: "системный фрод"}))

	f.mu.RLock()
	actualLen := len(f.stopWords)
	f.mu.RUnlock()

	require.Equal(t, 3, actualLen)
}

func TestStopListFilter_Delete(t *testing.T) {
	f, err := NewStopListFilter([]string{"скам", "вейп", "фрод"})
	require.NoError(t, err)

	err = f.Delete([]string{"ВЕЙП", "несуществующее"})
	require.NoError(t, err)

	require.True(t, f.Check(models.SearchEvent{Query: "купить вейп"}))
	require.False(t, f.Check(models.SearchEvent{Query: "кругом один скам"}))

	err = f.Delete([]string{"казино", "бэттинг"})
	require.ErrorIs(t, err, ErrWordsNotFound)

	err = f.Delete([]string{"скам", "фрод"})
	require.NoError(t, err)

	f.mu.RLock()
	machineIsNil := f.machine == nil
	f.mu.RUnlock()

	require.True(t, machineIsNil)
	require.True(t, f.Check(models.SearchEvent{Query: "проверка на скам и фрод"}))
}

func TestStopListFilter_Concurrency(t *testing.T) {
	f, err := NewStopListFilter([]string{"базовое_слово"})
	require.NoError(t, err)

	var wg sync.WaitGroup
	start := make(chan struct{})

	workers := 50
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				_ = f.Check(models.SearchEvent{Query: "обычный поисковый запрос пользователя"})
				_ = f.Check(models.SearchEvent{Query: "запрос содержащий базовое_слово"})
			}
		}()
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				word := []string{"динамический_запрос"}
				_ = f.Add(word)
				_ = f.Delete(word)
			}
		}()
	}

	close(start)
	wg.Wait()
}
