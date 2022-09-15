package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	authorizationHeaders = []string{"Authorization", "Cookie"}
	authTimeout          = 3 * time.Second

	authRequestDuration = metrics.Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: metrics.FQName("auth_request_duration_seconds"),
			Help: "Duration of performed authorization requests in seconds",
		},
		[]string{"code", "method"},
	)
	httpClient = &http.Client{
		Transport: promhttp.InstrumentRoundTripperDuration(authRequestDuration, http.DefaultTransport),
	}
)

func authorization(authUrl string) middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		ctx, cancel := context.WithTimeout(r.Context(), authTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, r.Method, authUrl, nil)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		req.Header.Set("X-Original-Uri", req.URL.String())
		if streamID := apiParam(r, streamIDParam); streamID != "" {
			req.Header.Set("X-Livepeer-Stream-Id", streamID)
		} else if assetID := apiParam(r, assetIDParam); assetID != "" {
			req.Header.Set("X-Livepeer-Asset-Id", assetID)
		}
		for _, header := range authorizationHeaders {
			req.Header[header] = r.Header[header]
		}
		res, err := httpClient.Do(req)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error authorizing request: %w", err))
			return
		}

		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent {
			if contentType := res.Header.Get("Content-Type"); contentType != "" {
				rw.Header().Set("Content-Type", contentType)
			}
			rw.WriteHeader(res.StatusCode)
			if _, err := io.Copy(rw, res.Body); err != nil {
				glog.Errorf("Error writing auth error response. err=%q, status=%d, headers=%+v", err, res.StatusCode, res.Header)
			}
			return
		}
		next.ServeHTTP(rw, r)
	})
}
