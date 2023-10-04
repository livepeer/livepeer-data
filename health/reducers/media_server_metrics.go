package reducers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	MetricViewerCount                data.MetricName = "ViewerCount"
	MetricMediaTimeMillis            data.MetricName = "MediaTimeMillis"
	MetricMultistreamMediaTimeMillis data.MetricName = "MultistreamMediaTimeMillis"
	MetricMultistreamActiveSec       data.MetricName = "MultistreamActiveSec"
	MetricMultistreamBytes           data.MetricName = "MultistreamBytes"
	MetricMultistreamBitrateSec      data.MetricName = "MultistreamBitrateSec"

	mediaServerExchange = "lp_mist_api_connector"
	metricsBindingKey   = "stream.metrics.#"
)

type MediaServerMetrics struct{}

func (t MediaServerMetrics) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: mediaServerExchange, Key: metricsBindingKey}}
}

func (t MediaServerMetrics) Conditions() []data.ConditionType {
	return nil
}

func (t MediaServerMetrics) Reduce(current *data.HealthStatus, _ interface{}, evtIface data.Event) (*data.HealthStatus, interface{}) {
	evt, ok := evtIface.(*data.MediaServerMetricsEvent)
	if !ok {
		return current, nil
	}

	metrics := current.MetricsCopy()
	ts, dims := evt.Timestamp(), map[string]string{"region": evt.Region, "nodeId": evt.NodeID}
	if evt.Stats != nil && evt.Stats.MediaTimeMs != nil {
		metrics.Add(data.NewMetric(MetricMediaTimeMillis, dims, ts, float64(*evt.Stats.MediaTimeMs)))
	}
	for _, ms := range evt.Multistream {
		for _, metric := range multistreamMetrics(current, ts, evt.NodeID, evt.Region, ms) {
			metrics.Add(metric)
		}
	}

	if vc := totalViewerCount(metrics); vc > 10 {
		glog.Warningf("High viewer count stream! streamId=%q viewerCount=%d", evt.StreamID(), vc)
	}
	return data.NewMergedHealthStatus(current, data.HealthStatus{Metrics: metrics}), nil
}

func multistreamMetrics(current *data.HealthStatus, ts time.Time, nodeID, region string, ms *data.MultistreamTargetMetrics) []*data.Metric {
	if ms == nil || ms.Metrics == nil {
		return nil
	}
	msDims := map[string]string{
		"nodeId":        nodeID,
		"region":        region,
		"targetId":      ms.Target.ID,
		"targetName":    ms.Target.Name,
		"targetProfile": ms.Target.Profile,
	}
	metrics := []*data.Metric{
		data.NewMetric(MetricMultistreamMediaTimeMillis, msDims, ts, float64(ms.Metrics.MediaTimeMs)),
		data.NewMetric(MetricMultistreamActiveSec, msDims, ts, float64(ms.Metrics.ActiveSec)),
		data.NewMetric(MetricMultistreamBytes, msDims, ts, float64(ms.Metrics.Bytes)),
	}
	if prevBytes := current.Metrics.GetMetric(MetricMultistreamBytes, msDims); prevBytes != nil {
		prevTs, prevVal := prevBytes.Last.Timestamp, int64(prevBytes.Last.Value)
		bitrate := float64(ms.Metrics.Bytes-prevVal) / float64(ts.Sub(prevTs)/time.Second)
		metrics = append(metrics, data.NewMetric(MetricMultistreamBitrateSec, msDims, ts, bitrate))
	}
	return metrics
}

func totalViewerCount(metrics data.MetricsMap) int {
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
