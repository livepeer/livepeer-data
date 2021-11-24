package reducers

import (
	"time"

	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/stats"
)

type statsAggrs struct {
	HealthStats    stats.WindowAggregators
	ConditionStats map[data.ConditionType]stats.WindowAggregators
}

func StatsReducer(statsWindows []time.Duration) health.ReducerFunc {
	return func(current *data.HealthStatus, stateIface interface{}, evt data.Event) (*data.HealthStatus, interface{}) {
		var state *statsAggrs
		if stateIface != nil {
			state = stateIface.(*statsAggrs)
		} else {
			state = &statsAggrs{
				HealthStats:    stats.WindowAggregators{},
				ConditionStats: map[data.ConditionType]stats.WindowAggregators{},
			}
		}

		ts := evt.Timestamp()
		conditions := current.ConditionsCopy()
		for i, cond := range conditions {
			statsAggr, ok := state.ConditionStats[cond.Type]
			if !ok {
				statsAggr = stats.WindowAggregators{}
				state.ConditionStats[cond.Type] = statsAggr
			}
			conditions[i] = reduceCondStats(cond, ts, statsAggr, statsWindows)
		}
		healthy := reduceCondStats(current.Healthy, ts, state.HealthStats, statsWindows)

		return data.NewMergedHealthStatus(current, data.HealthStatus{
			Healthy:    healthy,
			Conditions: conditions,
		}), state
	}
}

func reduceCondStats(cond *data.Condition, ts time.Time, statsAggr stats.WindowAggregators, statsWindows []time.Duration) *data.Condition {
	if cond.LastProbeTime == nil || cond.LastProbeTime.Time != ts {
		return cond
	}
	newCond := data.NewCondition(cond.Type, ts, cond.Status, cond)
	newCond.Frequency = statsAggr.Averages(statsWindows, ts, ptrBoolToFloat(cond.Status))
	return newCond
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
