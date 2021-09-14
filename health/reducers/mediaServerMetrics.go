package reducers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ViewerCountMetric health.MetricName = "ViewerCount"

	mediaServerExchange = "lp_mist_api_connector"
	metricsBindingKey   = "stream_metrics.#"
)

type MediaServerMetrics struct{}

func (t MediaServerMetrics) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: mediaServerExchange, Key: metricsBindingKey}}
}

func (t MediaServerMetrics) Conditions() []health.ConditionType {
	return nil
}

func (t MediaServerMetrics) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
	evt, ok := evtIface.(*data.MediaServerMetricsEvent)
	if !ok {
		return current, nil
	}

	ts := evt.Timestamp()
	dimensions := map[string]string{"nodeId": evt.NodeID}
	newMetrics := map[health.MetricName]float64{
		ViewerCountMetric: float64(evt.Stats.ViewerCount),
	}
	metrics := current.MetricsCopy().AddMetrics(dimensions, ts, newMetrics)

	if vc := totalViewerCount(metrics); vc > 10 {
		glog.Warning("High viewer count stream! streamId=%q viewerCount=%d", evt.StreamID(), vc)
	}
	return health.NewMergedStatus(current, health.Status{Metrics: metrics}), nil
}

func totalViewerCount(metrics health.MetricsMap) int {
	total := 0.0
	for _, metric := range metrics[ViewerCountMetric] {
		if time.Since(metric.Last.Timestamp) > 5*time.Minute {
			// ignore stale metrics from potentially old sessions.
			continue
		}
		total += metric.Last.Value
	}
	return int(total)
}
