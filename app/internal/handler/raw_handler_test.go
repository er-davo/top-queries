package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"top-queries/internal/api"
	"top-queries/internal/logger"
	"top-queries/internal/mocks"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestRawHandler_GetTopQueries(t *testing.T) {
	ctx := logger.ToContext(context.Background(), zap.NewNop())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTopList := mocks.NewMockRawTopQueriesList(ctrl)
	mockJSON := []byte(`{"data":[{"query":"микросервисы","count":10}],"updated_at":"2026-05-26T12:00:00Z"}`)

	mockTopList.EXPECT().GetTopJSON().Return(mockJSON).Times(1)

	bHandler := BaseHandler{}
	h := NewRawHandler(bHandler, mockTopList)

	resp, err := h.GetTopQueries(ctx, api.GetTopQueriesRequestObject{})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

type stubRawTopList struct {
	frozenData []byte
}

func (s *stubRawTopList) GetTopJSON() []byte {
	return s.frozenData
}

func BenchmarkRawHandler_GetTopQueries_FullStack(b *testing.B) {
	ctx := logger.ToContext(context.Background(), zap.NewNop())
	mockJSON := []byte(`{"data":[{"query":"go чистая архитектура","count":500}],"updated_at":"2026-05-26T12:00:00Z"}`)
	stub := &stubRawTopList{frozenData: mockJSON}

	h := NewRawHandler(BaseHandler{}, stub)
	strictHandler := api.NewStrictHandler(h, nil)

	req := httptest.NewRequest("GET", "/api/v1/top-queries", http.NoBody)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	limit := 10

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Body.Reset()
		strictHandler.GetTopQueries(w, req, api.GetTopQueriesParams{Limit: &limit})
	}
}
