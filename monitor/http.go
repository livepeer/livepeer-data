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
			Help: "Request duration of HTTP requests in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqTimeToHeaders = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: MetricName("http_request_time_to_headers_sec"),
			Help: "Time until HTTP headers are written, in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqInFlight = func(api string) prometheus.Gauge {
		return Factory.NewGauge(
			prometheus.GaugeOpts{
				Name:        MetricName("http_request_in_flight"),
				Help:        "Number of current requests in-flight for the specific API",
				ConstLabels: prometheus.Labels{"api": api},
			},
		)
	}
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
	handler = promhttp.InstrumentHandlerInFlight(
		httpReqInFlight(apiName),
		handler)
	return handler
}
