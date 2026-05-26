package handler

import (
	"context"

	"top-queries/internal/api"
	"top-queries/internal/logger"
	"top-queries/internal/metrics"
)

var _ api.StrictServerInterface = &StructHandler{}

// StructHandler implements the api.StrictServerInterface using structured data structures
// and dynamically rendering the JSON response with the requested item limits.
type StructHandler struct {
	BaseHandler
	structTopList StructedTopQueriesList
}

// NewStructHandler creates a new instance of StructHandler.
func NewStructHandler(
	bHandler BaseHandler,
	topList StructedTopQueriesList,
) *StructHandler {
	return &StructHandler{
		BaseHandler:   bHandler,
		structTopList: topList,
	}
}

// GetTopQueries fetches the top popular search queries up to the requested limit and dynamically converts them to the API model.
func (h *StructHandler) GetTopQueries(ctx context.Context, request api.GetTopQueriesRequestObject) (api.GetTopQueriesResponseObject, error) {
	metrics.HTTPRequestsTotal.Inc()

	l := logger.FromContext(ctx)
	l.Debug("handling get top queries request")

	data := h.structTopList.GetTopN(*request.Params.Limit)

	return api.GetTopQueries200JSONResponse{
		Data:      toAPITopQueryItem(data.Data),
		UpdatedAt: data.UpdatedAt,
	}, nil
}
