package reducers

import (
	"fmt"

	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ConditionTranscoding       health.ConditionType = "Transcoding"
	ConditionTranscodeRealTime health.ConditionType = "TranscodeRealTime"
	ConditionTranscodeNoErrors health.ConditionType = "TranscodeNoErrors"

	MetricTranscodeRealtimeRatio health.MetricName = "TranscodeRealtimeRatio"

	transcodeBindingKeyFormat = "broadcaster.stream_health.transcode.%s.#"
)

var transcodeConditions = []health.ConditionType{ConditionTranscoding, ConditionTranscodeRealTime, ConditionTranscodeNoErrors}

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

func (t TranscodeReducer) Conditions() []health.ConditionType {
	return transcodeConditions
}

func (t TranscodeReducer) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
	evt, ok := evtIface.(*data.TranscodeEvent)
	if !ok {
		return current, nil
	}

	ts := evt.Timestamp()
	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if status := conditionStatus(evt, cond.Type); status != nil {
			conditions[i] = health.NewCondition(cond.Type, ts, status, nil, cond)
		}
	}
	dimensions := map[string]string{"nodeId": evt.NodeID}
	metrics := current.MetricsCopy().Add(health.NewMetric(MetricTranscodeRealtimeRatio, dimensions, ts, realtimeRatio(evt)))

	return health.NewMergedStatus(current, health.Status{
		Conditions: conditions,
		Metrics:    metrics,
	}), nil
}

func conditionStatus(evt *data.TranscodeEvent, condType health.ConditionType) *bool {
	switch condType {
	case ConditionTranscoding:
		return &evt.Success
	case ConditionTranscodeRealTime:
		isRealTime := realtimeRatio(evt) >= 1
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

func realtimeRatio(evt *data.TranscodeEvent) float64 {
	return float64(evt.Segment.Duration*1000) / float64(evt.LatencyMs)
}
