package api

import (
	"context"
	"net/http"

	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

type contextKey int

const (
	streamStatusKey contextKey = iota
)

func streamStatus(healthcore *health.Core) middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		streamID := apiParam(r, streamIDParam)
		if streamID == "" {
			next.ServeHTTP(rw, r)
			return
		}
		status, err := healthcore.GetStatus(streamID)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), streamStatusKey, status))
		next.ServeHTTP(rw, r)
	})
}

func getStreamStatus(r *http.Request) *data.HealthStatus {
	return r.Context().Value(streamStatusKey).(*data.HealthStatus)
}
