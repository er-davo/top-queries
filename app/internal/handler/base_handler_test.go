package handler

import (
	"context"
	"errors"
	"testing"

	"top-queries/internal/api"
	"top-queries/internal/filters"
	"top-queries/internal/logger"
	"top-queries/internal/mocks"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func setupTestCtx() context.Context {
	return logger.ToContext(context.Background(), zap.NewNop())
}

func TestBaseHandler_AddStopWords(t *testing.T) {
	ctx := setupTestCtx()

	tests := []struct {
		name          string
		inputWords    []string
		mockExpect    func(m *mocks.MockStopWordList)
		expectedResp  api.AddStopWordsResponseObject
		expectedError error
	}{
		{
			name:       "success",
			inputWords: []string{"скам", "крипта"},
			mockExpect: func(m *mocks.MockStopWordList) {
				m.EXPECT().Add([]string{"скам", "крипта"}).Return(nil)
			},
			expectedResp: api.AddStopWords201JSONResponse{
				Success: true,
			},
			expectedError: nil,
		},
		{
			name:       "internal error",
			inputWords: []string{"казино"},
			mockExpect: func(m *mocks.MockStopWordList) {
				m.EXPECT().Add([]string{"казино"}).Return(errors.New("redis connection refused"))
			},
			expectedResp: api.AddStopWords500JSONResponse{
				Error: "Ошибка при добавлении слов",
				Code:  api.INTERNALERROR,
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSwList := mocks.NewMockStopWordList(ctrl)
			tt.mockExpect(mockSwList)

			h := NewBaseHandler(mockSwList)
			req := api.AddStopWordsRequestObject{
				Body: &api.StopWordsRequest{Words: tt.inputWords},
			}

			resp, err := h.AddStopWords(ctx, req)

			assert.Equal(t, tt.expectedError, err)
			assert.Equal(t, tt.expectedResp, resp)
		})
	}
}

func TestBaseHandler_DeleteStopWords(t *testing.T) {
	ctx := setupTestCtx()

	tests := []struct {
		name          string
		inputWords    []string
		mockExpect    func(m *mocks.MockStopWordList)
		expectedResp  api.DeleteStopWordsResponseObject
		expectedError error
	}{
		{
			name:       "success",
			inputWords: []string{"delete_me"},
			mockExpect: func(m *mocks.MockStopWordList) {
				m.EXPECT().Delete([]string{"delete_me"}).Return(nil)
			},
			expectedResp:  api.DeleteStopWords200JSONResponse{Success: true},
			expectedError: nil,
		},
		{
			name:       "not found error",
			inputWords: []string{"missing_word"},
			mockExpect: func(m *mocks.MockStopWordList) {
				m.EXPECT().Delete([]string{"missing_word"}).Return(filters.ErrWordsNotFound)
			},
			expectedResp: api.DeleteStopWords404JSONResponse{
				Error: "Слова не найдены",
				Code:  api.WORDSNOTFOUND,
			},
			expectedError: nil,
		},
		{
			name:       "internal error",
			inputWords: []string{"bug"},
			mockExpect: func(m *mocks.MockStopWordList) {
				m.EXPECT().Delete([]string{"bug"}).Return(errors.New("unexpected storage error"))
			},
			expectedResp: api.DeleteStopWords500JSONResponse{
				Error: "Ошибка при удалении слов",
				Code:  api.INTERNALERROR,
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSwList := mocks.NewMockStopWordList(ctrl)
			tt.mockExpect(mockSwList)

			h := NewBaseHandler(mockSwList)
			req := api.DeleteStopWordsRequestObject{
				Body: &api.StopWordsRequest{Words: tt.inputWords},
			}

			resp, err := h.DeleteStopWords(ctx, req)

			assert.Equal(t, tt.expectedError, err)
			assert.Equal(t, tt.expectedResp, resp)
		})
	}
}
