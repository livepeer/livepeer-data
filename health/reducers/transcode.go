package reducers

import (
	"fmt"

	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ConditionTranscoding       data.ConditionType = "Transcoding"
	ConditionTranscodeRealTime data.ConditionType = "TranscodeRealTime"
	ConditionTranscodeNoErrors data.ConditionType = "TranscodeNoErrors"

	MetricTranscodeRealtimeRatio data.MetricName = "TranscodeRealtimeRatio"

	transcodeBindingKeyFormat = "broadcaster.stream_health.transcode.%s.#"
)

var transcodeConditions = []data.ConditionType{ConditionTranscoding, ConditionTranscodeRealTime, ConditionTranscodeNoErrors}

type TranscodeReducer struct {
	GolpExchange  string
	ShardPrefixes []string
}

func (t TranscodeReducer) Bindings() []event.BindingArgs {
	if len(t.ShardPrefixes) == 0 {
		return []event.BindingArgs{{
			Exchange: t.GolpExchange,
			Key:      fmt.Sprintf(transcodeBindingKeyFormat, "*"),
		}}
	}
	bindings := make([]event.BindingArgs, len(t.ShardPrefixes))
	for i, prefix := range t.ShardPrefixes {
		bindings[i] = event.BindingArgs{
			Exchange: t.GolpExchange,
			Key:      fmt.Sprintf(transcodeBindingKeyFormat, prefix),
		}
	}
	return bindings
}

func (t TranscodeReducer) Conditions() []data.ConditionType {
	return transcodeConditions
}

func (t TranscodeReducer) Reduce(current *data.HealthStatus, _ interface{}, evtIface data.Event) (*data.HealthStatus, interface{}) {
	evt, ok := evtIface.(*data.TranscodeEvent)
	if !ok {
		return current, nil
	}

	ts := evt.Timestamp()
	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if status := conditionStatus(evt, cond.Type); status != nil {
			conditions[i] = data.NewCondition(cond.Type, ts, status, cond)
		}
	}
	dimensions := map[string]string{"nodeId": evt.NodeID}
	var metrics data.MetricsMap
	if rtRatio, ok := realtimeRatio(evt); ok {
		metrics = current.MetricsCopy().Add(data.NewMetric(MetricTranscodeRealtimeRatio, dimensions, ts, rtRatio))
	}

	return data.NewMergedHealthStatus(current, data.HealthStatus{
		Conditions: conditions,
		Metrics:    metrics,
	}), nil
}

func conditionStatus(evt *data.TranscodeEvent, condType data.ConditionType) *bool {
	switch condType {
	case ConditionTranscoding:
		return &evt.Success
	case ConditionTranscodeRealTime:
		ratio, ok := realtimeRatio(evt)
		isRealTime := ok && ratio >= 1
		return &isRealTime
	case ConditionTranscodeNoErrors:
		noErrors := true
		for _, attempt := range evt.Attempts {
			noErrors = noErrors && attempt.Error == nil
		}
		return &noErrors
	default:
		return nil
	}
}

func realtimeRatio(evt *data.TranscodeEvent) (float64, bool) {
	if evt.LatencyMs == 0 {
		return 0, false
	}
	return float64(evt.Segment.Duration*1000) / float64(evt.LatencyMs), true
}
