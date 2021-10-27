package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/nbio/hitch"
)


func authorization(authUrl string) hitch.Middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		// TODO: Move all this to a proper client
		status := getStreamStatus(r)
		authReq := map[string]interface{}{
			"resource": map[string]string{
				"method":   r.Method,
				"url":      r.URL.String(),
				"streamID": status.ID,
			},
		}
		payload, err := json.Marshal(authReq)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, fmt.Errorf("error creating authorization payload: %w", err))
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), "POST", authUrl, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		for _, header := range []string{"Authorization", "Proxy-Authorization", "Cookie"} {
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
