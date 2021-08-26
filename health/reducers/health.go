package reducers

import (
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

var healthyRequirementDefaults = map[health.ConditionType]bool{
	ConditionTranscoding:       false,
	ConditionTranscodeRealTime: false,
	ConditionMultistreaming:    true,
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current *health.Status, _ interface{}, evt data.Event) (*health.Status, interface{}) {
	isHealthy := true
	for _, cond := range current.Conditions {
		status, isRequired := healthyRequirementDefaults[cond.Type]
		if !isRequired {
			continue
		}
		if cond.Status != nil {
			status = *cond.Status
		}
		if !status {
			isHealthy = false
			break
		}
	}
	healthyCond := health.NewCondition("", evt.Timestamp(), &isHealthy, nil, current.Healthy)

	return health.NewMergedStatus(current, health.Status{Healthy: healthyCond}), nil
}
