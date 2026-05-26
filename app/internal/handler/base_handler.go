package handler

import (
	"context"
	"errors"

	"top-queries/internal/api"
	"top-queries/internal/filters"
	"top-queries/internal/logger"

	"go.uber.org/zap"
)

// BaseHandler implements the strict server interface for stop words management.
type BaseHandler struct {
	swList StopWordList
}

// NewBaseHandler creates a new instance of BaseHandler.
func NewBaseHandler(swList StopWordList) *BaseHandler {
	return &BaseHandler{
		swList: swList,
	}
}

// DeleteStopWords handles the deletion of specified words from the active stop list filter.
func (h *BaseHandler) DeleteStopWords(ctx context.Context, request api.DeleteStopWordsRequestObject) (api.DeleteStopWordsResponseObject, error) {
	l := logger.FromContext(ctx)

	l.Info("handling delete stop words request",
		zap.Int("words_count", len(request.Body.Words)),
		zap.Strings("words", request.Body.Words),
	)

	if err := h.swList.Delete(request.Body.Words); err != nil {
		if errors.Is(err, filters.ErrWordsNotFound) {
			l.Warn("failed to delete stop words: words not found",
				zap.Strings("words", request.Body.Words),
			)
			return api.DeleteStopWords404JSONResponse{
				Error: "Слова не найдены",
				Code:  api.WORDSNOTFOUND,
			}, nil
		}

		l.Error("failed to delete stop words: internal error",
			zap.Error(err),
			zap.Strings("words", request.Body.Words),
		)
		return api.DeleteStopWords500JSONResponse{
			Error: "Ошибка при удалении слов",
			Code:  api.INTERNALERROR,
		}, nil
	}

	l.Info("successfully deleted stop words", zap.Strings("words", request.Body.Words))
	return api.DeleteStopWords200JSONResponse{
		Success: true,
	}, nil
}

// AddStopWords handles appending new terms to the active stop list filter.
func (h *BaseHandler) AddStopWords(ctx context.Context, request api.AddStopWordsRequestObject) (api.AddStopWordsResponseObject, error) {
	l := logger.FromContext(ctx)

	l.Info("handling add stop words request",
		zap.Int("words_count", len(request.Body.Words)),
		zap.Strings("words", request.Body.Words),
	)

	if err := h.swList.Add(request.Body.Words); err != nil {
		l.Error("failed to add stop words: internal error",
			zap.Error(err),
			zap.Strings("words", request.Body.Words),
		)
		return api.AddStopWords500JSONResponse{
			Error: "Ошибка при добавлении слов",
			Code:  api.INTERNALERROR,
		}, nil
	}

	l.Info("successfully added stop words", zap.Strings("words", request.Body.Words))
	return api.AddStopWords201JSONResponse{
		Success: true,
	}, nil
}
