package reducers

import (
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

var healthyRequirementDefaults = map[health.ConditionType]bool{
	ConditionTranscoding:        false,
	ConditionRealTime:           false,
	ConditionMultistreamHealthy: true,
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current *health.Status, _ interface{}, evt data.Event) (*health.Status, interface{}) {
	healthyMustsCount := 0
	for _, cond := range current.Conditions {
		status, isRequired := healthyRequirementDefaults[cond.Type]
		if cond.Status != nil {
			status = *cond.Status
		}
		if isRequired && status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyRequirementDefaults)
	healthyCond := health.NewCondition("", evt.Timestamp(), &isHealthy, nil, current.Healthy)

	return health.NewMergedStatus(current, health.Status{Healthy: healthyCond}), nil
}
