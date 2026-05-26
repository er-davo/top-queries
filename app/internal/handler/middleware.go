package handler

import (
	"net/http"

	"top-queries/internal/logger"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// LoggerMiddleware injects a request-scoped zap.Logger populated with fields
// like request_id, method, and path into the request context.
func LoggerMiddleware(baseLogger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			reqID := middleware.GetReqID(ctx)

			fields := []zap.Field{
				zap.String("request_id", reqID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
			}

			enrichedLogger := baseLogger.With(fields...)
			lCtx := logger.ToContext(ctx, enrichedLogger)

			next.ServeHTTP(w, r.WithContext(lCtx))
		})
	}
}
