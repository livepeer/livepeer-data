package api

import (
	"context"
	"net/http"

	"github.com/livepeer/livepeer-data/health"
	"github.com/nbio/hitch"
)

type contextKey int

const (
	streamStatusKey contextKey = iota
)

func streamStatus(healthcore *health.Core, streamIDParam string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		params := hitch.Params(r)
		streamID := params.ByName(streamIDParam)
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

func getStreamStatus(r *http.Request) *health.Status {
	return r.Context().Value(streamStatusKey).(*health.Status)
}
