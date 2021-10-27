package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/nbio/hitch"
)

var authorizationHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie"}

func authorization(authUrl string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		status := getStreamStatus(r)
		req, err := http.NewRequestWithContext(r.Context(), r.Method, authUrl, nil)
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
			outerErr := fmt.Errorf("authorization failed: %d %s", res.StatusCode, res.Status)
			var errResp errorResponse
			if err := json.NewDecoder(res.Body).Decode(&errResp); err == nil && len(errResp.Errors) > 0 {
				outerErr = fmt.Errorf("authorization failed: %s", strings.Join(errResp.Errors, "; "))
			}
			respondError(rw, res.StatusCode, outerErr)
			return
		}
		next.ServeHTTP(rw, r)
	})
}
