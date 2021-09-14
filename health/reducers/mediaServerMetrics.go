package reducers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	MetricViewerCount                health.MetricName = "ViewerCount"
	MetricMediaTimeMillis            health.MetricName = "MediaTimeMillis"
	MetricMultistreamMediaTimeMillis health.MetricName = "MultistreamMediaTimeMillis"
	MetricMultistreamActiveSec       health.MetricName = "MultistreamActiveSec"
	MetricMultistreamBytes           health.MetricName = "MultistreamBytes"
	MetricMultistreamBitrateSec      health.MetricName = "MultistreamBitrateSec"

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

	ts, dims := evt.Timestamp(), map[string]string{"nodeId": evt.NodeID}
	newMetrics := []*health.Metric{
		health.NewMetric(MetricViewerCount, dims, ts, float64(evt.Stats.ViewerCount), nil),
	}
	if evt.Stats.MediaTimeMs != nil {
		newMetrics = append(newMetrics, health.NewMetric(MetricMediaTimeMillis, dims, ts, float64(*evt.Stats.MediaTimeMs), nil))
	}
	for _, ms := range evt.Multistream {
		newMetrics = append(newMetrics, multistreamMetrics(current, ts, evt.NodeID, ms)...)
	}
	metrics := current.MetricsCopy().AddMetrics(newMetrics...)

	if vc := totalViewerCount(metrics); vc > 10 {
		glog.Warning("High viewer count stream! streamId=%q viewerCount=%d", evt.StreamID(), vc)
	}
	return health.NewMergedStatus(current, health.Status{Metrics: metrics}), nil
}

func multistreamMetrics(current *health.Status, ts time.Time, nodeID string, ms *data.MultistreamTargetMetrics) []*health.Metric {
	msDims := map[string]string{
		"nodeId":        nodeID,
		"targetId":      ms.Target.ID,
		"targetName":    ms.Target.Name,
		"targetProfile": ms.Target.Profile,
	}
	metrics := []*health.Metric{
		health.NewMetric(MetricMultistreamMediaTimeMillis, msDims, ts, float64(ms.Metrics.MediaTimeMs), nil),
		health.NewMetric(MetricMultistreamActiveSec, msDims, ts, float64(ms.Metrics.ActiveSec), nil),
		health.NewMetric(MetricMultistreamBytes, msDims, ts, float64(ms.Metrics.Bytes), nil),
	}
	if prevBytes := current.Metrics.GetMetric(MetricMultistreamBytes, msDims); prevBytes != nil {
		prevTs, prevVal := prevBytes.Last.Timestamp, int64(prevBytes.Last.Value)
		bitrate := float64(ms.Metrics.Bytes-prevVal) / float64(ts.Sub(prevTs)/time.Second)
		metrics = append(metrics, health.NewMetric(MetricMultistreamBitrateSec, msDims, ts, bitrate, nil))
	}
	return metrics
}

func totalViewerCount(metrics health.MetricsMap) int {
	total := 0.0
	for _, metric := range metrics[MetricViewerCount] {
		if time.Since(metric.Last.Timestamp) > 5*time.Minute {
			// ignore stale metrics from potentially old sessions.
			continue
		}
		total += metric.Last.Value
	}
	return int(total)
}
