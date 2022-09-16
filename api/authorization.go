package api

import (
	"context"
	"encoding/json"
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
	authorizationHeaders = []string{"Authorization", "Cookie", "Origin"}
	// the response headers proxied from the auth request are basically cors headers
	proxiedResponseHeaders = []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Credentials",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
		"Access-Control-Expose-Headers",
		"Access-Control-Max-Age",
	}
	authTimeout = 3 * time.Second

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

		authReq, err := http.NewRequestWithContext(ctx, r.Method, authUrl, nil)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		authReq.Header.Set("X-Original-Uri", originalReqUri(r))
		if streamID := apiParam(r, streamIDParam); streamID != "" {
			authReq.Header.Set("X-Livepeer-Stream-Id", streamID)
		} else if assetID := apiParam(r, assetIDParam); assetID != "" {
			authReq.Header.Set("X-Livepeer-Asset-Id", assetID)
		}
		copyHeaders(authorizationHeaders, r.Header, authReq.Header)

		hs, _ := json.Marshal(authReq.Header)
		glog.Infof("Performing auth request: %s %s headers=%s", authReq.Method, authReq.URL, hs)
		inHs, _ := json.Marshal(r.Header)
		glog.Infof("auth: From request: %s %s headers=%s", r.Method, r.URL, inHs)

		authRes, err := httpClient.Do(authReq)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error authorizing request: %w", err))
			return
		}
		copyHeaders(proxiedResponseHeaders, authRes.Header, rw.Header())

		if authRes.StatusCode != http.StatusOK && authRes.StatusCode != http.StatusNoContent {
			if contentType := authRes.Header.Get("Content-Type"); contentType != "" {
				rw.Header().Set("Content-Type", contentType)
			}
			rw.WriteHeader(authRes.StatusCode)
			if _, err := io.Copy(rw, authRes.Body); err != nil {
				glog.Errorf("Error writing auth error response. err=%q, status=%d, headers=%+v", err, authRes.StatusCode, authRes.Header)
			}
			return
		}
		next.ServeHTTP(rw, r)
	})
}

func originalReqUri(r *http.Request) string {
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	if fwdProto := r.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
		proto = fwdProto
	}
	return fmt.Sprintf("%s://%s%s", proto, r.Host, r.URL.RequestURI())
}

func copyHeaders(headers []string, src, dest http.Header) {
	for _, header := range headers {
		if vals := src[header]; len(vals) > 0 {
			dest[header] = vals
		}
	}
}
