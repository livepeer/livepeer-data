package reducers

import (
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

var healthyRequirementDefaults = map[data.ConditionType]bool{
	ConditionTranscoding:       false,
	ConditionTranscodeRealTime: false,
	ConditionMultistreaming:    true,
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current *data.HealthStatus, _ interface{}, evt data.Event) (*data.HealthStatus, interface{}) {
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
	healthyCond := data.NewCondition("", evt.Timestamp(), &isHealthy, current.Healthy)

	return data.NewMergedHealthStatus(current, data.HealthStatus{Healthy: healthyCond}), nil
}
