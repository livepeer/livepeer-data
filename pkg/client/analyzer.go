// Package client contains clients for the Stream Health analyzer public API.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
)

type (
	// Analyzer abstracts the stream health analyzer APIs.
	Analyzer interface {
		GetStreamHealth(ctx context.Context, streamID string) (*data.HealthStatus, error)
	}

	errorResponse struct {
		Errors []string `json:"errors"`
	}

	analyzer struct {
		baseUrl    string
		userAgent  string
		authToken  string
		httpClient *http.Client
	}
)

// NewAnalyzer creates a client for the stream health analyzer service.
func NewAnalyzer(baseUrl, authToken, userAgent string, timeout time.Duration) Analyzer {
	if timeout <= 0 {
		timeout = 4 * time.Second
	}
	return &analyzer{
		baseUrl:   addScheme(baseUrl),
		authToken: authToken,
		userAgent: userAgent,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (a *analyzer) GetStreamHealth(ctx context.Context, streamID string) (*data.HealthStatus, error) {
	url := fmt.Sprintf("%s/data/stream/%s/health", a.baseUrl, streamID)
	body, err := a.doGet(ctx, url)
	if err != nil {
		return nil, err
	}

	var health *data.HealthStatus
	if err = json.Unmarshal(body, &health); err != nil {
		glog.Errorf("Error parsing stream health response from Analyzer url=%q, err=%q, body=%q", url, err, string(body))
		return nil, err
	}
	return health, nil
}

func (a *analyzer) doGet(ctx context.Context, url string) ([]byte, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Add("Authorization", "Bearer "+a.authToken)
	if a.userAgent != "" {
		req.Header.Add("User-Agent", a.userAgent)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Get request error to analyzer url=%q, err=%q", url, err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading analyzer response body url=%q, status=%d, error=%q", url, resp.StatusCode, err)
		return nil, err
	}
	if glog.V(7) {
		took := time.Since(start)
		glog.Infof("Analyzer get request done url=%q, status=%d, latency=%v, body=%q",
			url, resp.StatusCode, took, string(body))
	}
	if resp.StatusCode != http.StatusOK {
		glog.Errorf("Status error from analyzer url=%q, status=%d, body=%q", url, resp.StatusCode, string(body))
		var errResp errorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			glog.Errorf("Failed to parse error response url=%q, status=%d, err=%q", url, resp.StatusCode, err)
			errResp.Errors = []string{string(body)}
		}
		return nil, APIError{resp.StatusCode, errResp.Errors}
	}
	return body, nil
}

func addScheme(url string) string {
	url = strings.ToLower(url)
	if url == "" || strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}
	if strings.Contains(url, ".local") || strings.HasPrefix(url, "localhost") {
		return "http://" + url
	}
	return "https://" + url
}
