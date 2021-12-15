package monitor

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpReqDuration = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: MetricName("http_request_duration_sec"),
			Help: "Request duration of HTTP requests in seconds.",
		},
		[]string{"code", "method", "api"},
	)
	httpReqTimeToHeaders = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: MetricName("http_request_time_to_headers_sec"),
			Help: "Time until HTTP headers are written, in seconds.",
		},
		[]string{"code", "method", "api"},
	)
)

func ObservedHandler(apiName string, handler http.Handler) http.Handler {
	if !inited {
		return handler
	}
	handler = promhttp.InstrumentHandlerTimeToWriteHeader(
		httpReqTimeToHeaders.MustCurryWith(prometheus.Labels{"api": apiName}),
		handler)
	handler = promhttp.InstrumentHandlerDuration(
		httpReqDuration.MustCurryWith(prometheus.Labels{"api": apiName}),
		handler)
	return handler
}
