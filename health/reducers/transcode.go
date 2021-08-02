package reducers

import (
	"fmt"
	"strings"

	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ConditionTranscoding health.ConditionType = "Transcoding"
	ConditionRealTime    health.ConditionType = "RealTime"
	ConditionNoErrors    health.ConditionType = "NoErrors"

	transcodeBindingKeyFormat = "*.stream_health.transcode.%s.*"
)

var transcodeConditions = []health.ConditionType{ConditionTranscoding, ConditionRealTime, ConditionNoErrors}

type TranscodeReducer struct {
	GolpExchange  string
	ShardPrefixes string
}

func (t TranscodeReducer) Bindings() []event.BindingArgs {
	if t.ShardPrefixes == "" {
		return []event.BindingArgs{{
			Key:      fmt.Sprintf(transcodeBindingKeyFormat, "*"),
			Exchange: t.GolpExchange,
		}}
	}
	prefixes := strings.Split(t.ShardPrefixes, ",")
	bindings := make([]event.BindingArgs, len(prefixes))
	for i, prefix := range prefixes {
		bindings[i] = event.BindingArgs{
			Key:      fmt.Sprintf(transcodeBindingKeyFormat, prefix),
			Exchange: t.GolpExchange,
		}
	}
	return bindings
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
		ID:         current.ID,
		Healthy:    current.Healthy,
		Conditions: conditions,
	}, nil
}

func conditionStatus(evt *health.TranscodeEvent, condType health.ConditionType) *bool {
	switch condType {
	case ConditionTranscoding:
		return &evt.Success
	case ConditionRealTime:
		isRealTime := evt.LatencyMs < int64(evt.Segment.Duration*1000)
		return &isRealTime
	case ConditionNoErrors:
		noErrors := true
		for _, attempt := range evt.Attempts {
			noErrors = noErrors && attempt.Error == nil
		}
		return &noErrors
	default:
		return nil
	}
}
