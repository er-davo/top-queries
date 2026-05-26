package handler

import (
	"context"

	"top-queries/internal/api"
	"top-queries/internal/logger"
	"top-queries/internal/metrics"
)

var _ api.StrictServerInterface = &RawHandler{}

// RawHandler implements the api.StrictServerInterface using pre-rendered raw JSON data
// for maximum throughput and minimal allocation overhead.
type RawHandler struct {
	BaseHandler
	rawTopList RawTopQueriesList
}

// NewRawHandler creates a new instance of RawHandler.
func NewRawHandler(
	bHandler BaseHandler,
	topList RawTopQueriesList,
) *RawHandler {
	return &RawHandler{
		BaseHandler: bHandler,
		rawTopList:  topList,
	}
}

// GetTopQueries returns the pre-rendered top popular search queries directly from memory.
func (h *RawHandler) GetTopQueries(ctx context.Context, request api.GetTopQueriesRequestObject) (api.GetTopQueriesResponseObject, error) {
	metrics.HTTPRequestsTotal.Inc()

	l := logger.FromContext(ctx)
	l.Debug("handling get top queries request")

	data := h.rawTopList.GetTopJSON()

	return rawJSONResponse(data), nil
}
