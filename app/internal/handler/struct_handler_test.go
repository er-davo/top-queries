package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"top-queries/internal/api"
	"top-queries/internal/logger"
	"top-queries/internal/models"

	"go.uber.org/zap"
)

type stubStructTopList struct {
	frozenData models.TopQueriesResponseEnvelope
}

func (s *stubStructTopList) GetTopN(limit int) models.TopQueriesResponseEnvelope {
	if limit <= 0 || limit >= len(s.frozenData.Data) {
		return s.frozenData
	}
	return models.TopQueriesResponseEnvelope{
		Data:      s.frozenData.Data[:limit],
		UpdatedAt: s.frozenData.UpdatedAt,
	}
}

func BenchmarkStructHandler_GetTopQueries_FullStack(b *testing.B) {
	ctx := logger.ToContext(context.Background(), zap.NewNop())

	envelope := models.TopQueriesResponseEnvelope{
		Data: []models.TargetQuery{
			{Query: "go чистая архитектура", Count: 500},
			{Query: "купить гитару", Count: 300},
			{Query: "разработка высоконагруженных систем", Count: 150},
		},
		UpdatedAt: time.Now().UTC(),
	}
	stub := &stubStructTopList{frozenData: envelope}

	h := NewStructHandler(BaseHandler{}, stub)
	strictHandler := api.NewStrictHandler(h, nil)

	limit := 3
	req := httptest.NewRequest("GET", "/api/v1/top-queries", http.NoBody)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Body.Reset()
		strictHandler.GetTopQueries(w, req, api.GetTopQueriesParams{Limit: &limit})
	}
}
