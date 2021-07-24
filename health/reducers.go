package health

import (
	"time"

	"github.com/livepeer/healthy-streams/event"
	"github.com/livepeer/healthy-streams/stats"
)

const transcodeBindingKey = "#.stream_health.transcode.#"

var transcodeConditions = []ConditionType{ConditionTranscoding, ConditionRealTime, ConditionNoErrors}
var healthyMustHaves = map[ConditionType]bool{
	ConditionTranscoding: true,
	ConditionRealTime:    true,
}

type transcodeReducer struct{}

func (t transcodeReducer) Bindings(golpExchange string) []event.BindingArgs {
	return []event.BindingArgs{{Key: transcodeBindingKey, Exchange: golpExchange}}
}

func (t transcodeReducer) Conditions() []ConditionType {
	return transcodeConditions
}

func (t transcodeReducer) Reduce(current Status, _ interface{}, evtIface Event) (Status, interface{}) {
	evt, ok := evtIface.(*TranscodeEvent)
	if !ok {
		return current, nil
	}

	ts := evt.Timestamp()
	conditions := make([]*Condition, len(current.Conditions))
	for i, cond := range current.Conditions {
		status := conditionStatus(evt, cond.Type)
		if status == nil {
			conditions[i] = cond
			continue
		}
		conditions[i] = NewCondition(cond.Type, ts, status, nil, cond)
	}

	return Status{
		ManifestID: current.ManifestID,
		Healthy:    current.Healthy,
		Conditions: conditions,
	}, nil
}

func conditionStatus(evt *TranscodeEvent, condType ConditionType) *bool {
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

func ReduceHealth(current Status, _ interface{}, evt Event) (Status, interface{}) {
	healthyMustsCount := 0
	for _, cond := range current.Conditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthyCond := NewCondition("", evt.Timestamp(), &isHealthy, nil, &current.Healthy)

	return Status{
		ManifestID: current.ManifestID,
		Healthy:    *healthyCond,
		Conditions: current.Conditions,
	}, nil
}

type statsAggrs struct {
	HealthStats    stats.WindowAggregators
	ConditionStats map[ConditionType]stats.WindowAggregators
}

type statsReducer struct {
	StatsWindows []time.Duration
}

func (s statsReducer) Bindings(golpExchange string) []event.BindingArgs {
	return nil
}

func (s statsReducer) Conditions() []ConditionType {
	return nil
}

func (s statsReducer) Reduce(current Status, stateIface interface{}, evt Event) (Status, interface{}) {
	var state *statsAggrs
	if stateIface != nil {
		state = stateIface.(*statsAggrs)
	} else {
		state = &statsAggrs{
			HealthStats:    stats.WindowAggregators{},
			ConditionStats: map[ConditionType]stats.WindowAggregators{},
		}
	}

	ts := evt.Timestamp()
	conditions := make([]*Condition, len(current.Conditions))
	for i, cond := range current.Conditions {
		statsAggr, ok := state.ConditionStats[cond.Type]
		if !ok {
			statsAggr = stats.WindowAggregators{}
			state.ConditionStats[cond.Type] = statsAggr
		}
		conditions[i] = s.reduceCondStats(cond, ts, statsAggr)
	}
	return Status{
		ManifestID: current.ManifestID,
		Healthy:    *s.reduceCondStats(&current.Healthy, ts, state.HealthStats),
		Conditions: conditions,
	}, state
}

func (r statsReducer) reduceCondStats(cond *Condition, ts time.Time, statsAggr stats.WindowAggregators) *Condition {
	if cond.LastProbeTime == nil || *cond.LastProbeTime != ts {
		return cond
	}
	newCond := *cond
	newCond.Frequency = statsAggr.Averages(r.StatsWindows, ts, ptrBoolToFloat(cond.Status))
	return &newCond
}

func ptrBoolToFloat(b *bool) *float64 {
	if b == nil {
		return nil
	}
	val := float64(0)
	if *b {
		val = 1
	}
	return &val
}
