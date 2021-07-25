package reducers

import (
	"time"

	"github.com/livepeer/healthy-streams/health"
	"github.com/livepeer/healthy-streams/stats"
)

type statsAggrs struct {
	HealthStats    stats.WindowAggregators
	ConditionStats map[health.ConditionType]stats.WindowAggregators
}

func StatsReducer(statsWindows []time.Duration) health.ReducerFunc {
	return func(current health.Status, stateIface interface{}, evt health.Event) (health.Status, interface{}) {
		var state *statsAggrs
		if stateIface != nil {
			state = stateIface.(*statsAggrs)
		} else {
			state = &statsAggrs{
				HealthStats:    stats.WindowAggregators{},
				ConditionStats: map[health.ConditionType]stats.WindowAggregators{},
			}
		}

		ts := evt.Timestamp()
		conditions := make([]*health.Condition, len(current.Conditions))
		for i, cond := range current.Conditions {
			statsAggr, ok := state.ConditionStats[cond.Type]
			if !ok {
				statsAggr = stats.WindowAggregators{}
				state.ConditionStats[cond.Type] = statsAggr
			}
			conditions[i] = reduceCondStats(cond, ts, statsAggr, statsWindows)
		}
		return health.Status{
			ManifestID: current.ManifestID,
			Healthy:    *reduceCondStats(&current.Healthy, ts, state.HealthStats, statsWindows),
			Conditions: conditions,
		}, state
	}
}

func reduceCondStats(cond *health.Condition, ts time.Time, statsAggr stats.WindowAggregators, statsWindows []time.Duration) *health.Condition {
	if cond.LastProbeTime == nil || *cond.LastProbeTime != ts {
		return cond
	}
	newCond := *cond
	newCond.Frequency = statsAggr.Averages(statsWindows, ts, ptrBoolToFloat(cond.Status))
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
