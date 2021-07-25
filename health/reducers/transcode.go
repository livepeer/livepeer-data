package reducers

import (
	"github.com/livepeer/healthy-streams/event"
	"github.com/livepeer/healthy-streams/health"
)

const transcodeBindingKey = "#.stream_health.transcode.#"

var transcodeConditions = []health.ConditionType{health.ConditionTranscoding, health.ConditionRealTime, health.ConditionNoErrors}
var healthyMustHaves = map[health.ConditionType]bool{
	health.ConditionTranscoding: true,
	health.ConditionRealTime:    true,
}

type TranscodeReducer struct{}

func (t TranscodeReducer) Bindings(golpExchange string) []event.BindingArgs {
	return []event.BindingArgs{{Key: transcodeBindingKey, Exchange: golpExchange}}
}

func (t TranscodeReducer) Conditions() []health.ConditionType {
	return transcodeConditions
}

func (t TranscodeReducer) Reduce(current health.Status, _ interface{}, evtIface health.Event) (health.Status, interface{}) {
	evt, ok := evtIface.(*health.TranscodeEvent)
	if !ok {
		return current, nil
	}

	ts := evt.Timestamp()
	conditions := make([]*health.Condition, len(current.Conditions))
	for i, cond := range current.Conditions {
		status := conditionStatus(evt, cond.Type)
		if status == nil {
			conditions[i] = cond
			continue
		}
		conditions[i] = health.NewCondition(cond.Type, ts, status, nil, cond)
	}

	return health.Status{
		ManifestID: current.ManifestID,
		Healthy:    current.Healthy,
		Conditions: conditions,
	}, nil
}

func conditionStatus(evt *health.TranscodeEvent, condType health.ConditionType) *bool {
	switch condType {
	case health.ConditionTranscoding:
		return &evt.Success
	case health.ConditionRealTime:
		isRealTime := evt.LatencyMs < int64(evt.Segment.Duration*1000)
		return &isRealTime
	case health.ConditionNoErrors:
		noErrors := true
		for _, attempt := range evt.Attempts {
			noErrors = noErrors && attempt.Error == nil
		}
		return &noErrors
	default:
		return nil
	}
}
