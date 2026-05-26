package handler

import (
	"net/http"

	"top-queries/internal/api"
	"top-queries/internal/models"
)

type rawJSONResponse []byte

// VisitGetTopQueriesResponse flushes the pre-rendered JSON slice directly
// to the http.ResponseWriter, skipping the generic json.Marshal step.
func (r rawJSONResponse) VisitGetTopQueriesResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err := w.Write(r)
	return err
}

// toAPITopQueryItem maps core domain queries model into compliant OpenAPI schema items.
func toAPITopQueryItem(item []models.TargetQuery) []api.TopQueryItem {
	res := make([]api.TopQueryItem, len(item))
	for i, v := range item {
		res[i] = api.TopQueryItem{
			Query: v.Query,
			Count: v.Count,
		}
	}
	return res
}
