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
	metricsBindingKey   = "stream.metrics.#"
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

	metrics := current.MetricsCopy()
	ts, dims := evt.Timestamp(), map[string]string{"region": evt.Region, "nodeId": evt.NodeID}
	if evt.Stats.MediaTimeMs != nil {
		metrics.Add(health.NewMetric(MetricMediaTimeMillis, dims, ts, float64(*evt.Stats.MediaTimeMs)))
	}
	for _, ms := range evt.Multistream {
		for _, metric := range multistreamMetrics(current, ts, evt.NodeID, ms) {
			metrics.Add(metric)
		}
	}

	if vc := totalViewerCount(metrics); vc > 10 {
		glog.Warning("High viewer count stream! streamId=%q viewerCount=%d", evt.StreamID(), vc)
	}
	return health.NewMergedStatus(current, health.Status{Metrics: metrics}), nil
}

func multistreamMetrics(current *health.Status, ts time.Time, nodeID string, ms *data.MultistreamTargetMetrics) []*health.Metric {
	if ms.Metrics == nil {
		return nil
	}
	msDims := map[string]string{
		"nodeId":        nodeID,
		"targetId":      ms.Target.ID,
		"targetName":    ms.Target.Name,
		"targetProfile": ms.Target.Profile,
	}
	metrics := []*health.Metric{
		health.NewMetric(MetricMultistreamMediaTimeMillis, msDims, ts, float64(ms.Metrics.MediaTimeMs)),
		health.NewMetric(MetricMultistreamActiveSec, msDims, ts, float64(ms.Metrics.ActiveSec)),
		health.NewMetric(MetricMultistreamBytes, msDims, ts, float64(ms.Metrics.Bytes)),
	}
	if prevBytes := current.Metrics.GetMetric(MetricMultistreamBytes, msDims); prevBytes != nil {
		prevTs, prevVal := prevBytes.Last.Timestamp, int64(prevBytes.Last.Value)
		bitrate := float64(ms.Metrics.Bytes-prevVal) / float64(ts.Sub(prevTs)/time.Second)
		metrics = append(metrics, health.NewMetric(MetricMultistreamBitrateSec, msDims, ts, bitrate))
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
