package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/nbio/hitch"
)

var authorizationHeaders = []string{"Authorization", "Cookie"}
var authTimeout = 3 * time.Second

func authorization(authUrl string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		ctx, cancel := context.WithTimeout(r.Context(), authTimeout)
		defer cancel()

		status := getStreamStatus(r)
		req, err := http.NewRequestWithContext(ctx, r.Method, authUrl, nil)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		req.Header.Set("X-Original-Uri", req.URL.String())
		req.Header.Set("X-Livepeer-Stream-Id", status.ID)
		for _, header := range authorizationHeaders {
			req.Header[header] = r.Header[header]
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error authorizing request: %w", err))
			return
		}

		if res.StatusCode != http.StatusOK {
			rw.Header().Set("Content-Type", res.Header.Get("Content-Type"))
			rw.WriteHeader(res.StatusCode)
			if _, err := io.Copy(rw, res.Body); err != nil {
				glog.Errorf("Error writing auth error response. err=%q, status=%d, headers=%+v", err, res.StatusCode, res.Header)
			}
			return
		}
		next.ServeHTTP(rw, r)
	})
}
