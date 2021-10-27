package reducers

import (
	"time"

	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/stats"
)

type statsAggrs struct {
	HealthStats    stats.WindowAggregators
	ConditionStats map[health.ConditionType]stats.WindowAggregators
}

func StatsReducer(statsWindows []time.Duration) health.ReducerFunc {
	return func(current *health.Status, stateIface interface{}, evt data.Event) (*health.Status, interface{}) {
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

		return health.NewMergedStatus(current, health.Status{
			Healthy:    healthy,
			Conditions: conditions,
		}), state
	}
}

func reduceCondStats(cond *health.Condition, ts time.Time, statsAggr stats.WindowAggregators, statsWindows []time.Duration) *health.Condition {
	if cond.LastProbeTime == nil || cond.LastProbeTime.Time != ts {
		return cond
	}
	frequency := statsAggr.Averages(statsWindows, ts, ptrBoolToFloat(cond.Status))
	return health.NewCondition(cond.Type, ts, cond.Status, frequency, cond)
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
