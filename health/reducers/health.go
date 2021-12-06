package reducers

import (
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

var trueValue = true

type healthRequirement struct {
	condition data.ConditionType
	optional  bool
}

var healthRequirements = []healthRequirement{
	{ConditionTranscoding, false},
	{ConditionTranscodeRealTime, false},
	{ConditionMultistreaming, true},
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current *data.HealthStatus, _ interface{}, evt data.Event) (*data.HealthStatus, interface{}) {
	healthStatus := &trueValue
	for _, req := range healthRequirements {
		cond := current.Condition(req.condition)
		if cond == nil || cond.Status == nil {
			if !req.optional {
				healthStatus = nil
			}
			continue
		}
		if !*cond.Status {
			healthStatus = cond.Status
			break
		}
	}
	healthyCond := data.NewCondition("", evt.Timestamp(), healthStatus, current.Healthy)

	return data.NewMergedHealthStatus(current, data.HealthStatus{Healthy: healthyCond}), nil
}
