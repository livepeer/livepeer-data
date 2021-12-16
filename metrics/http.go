package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpReqDuration = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: FQName("http_request_duration_sec"),
			Help: "Request duration of HTTP requests in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqTimeToHeaders = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: FQName("http_request_time_to_headers_sec"),
			Help: "Time until HTTP headers are written, in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqInFlight = Factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: FQName("http_request_in_flight"),
			Help: "Number of current requests in-flight for the specific API",
		},
		[]string{"api"},
	)
)

func ObservedHandler(apiName string, handler http.Handler) http.Handler {
	apiLabel := prometheus.Labels{"api": apiName}
	handler = promhttp.InstrumentHandlerTimeToWriteHeader(
		httpReqTimeToHeaders.MustCurryWith(apiLabel),
		handler)
	handler = promhttp.InstrumentHandlerDuration(
		httpReqDuration.MustCurryWith(apiLabel),
		handler)
	handler = promhttp.InstrumentHandlerInFlight(
		httpReqInFlight.WithLabelValues(apiName),
		handler)
	return handler
}
