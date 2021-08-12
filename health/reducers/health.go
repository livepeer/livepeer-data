package reducers

import (
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
)

var healthyMustHaves = map[health.ConditionType]bool{
	ConditionTranscoding: true,
	ConditionRealTime:    true,
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current health.Status, _ interface{}, evt data.Event) (health.Status, interface{}) {
	healthyMustsCount := 0
	for _, cond := range current.Conditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthyCond := health.NewCondition("", evt.Timestamp(), &isHealthy, nil, &current.Healthy)

	return health.Status{
		ID:         current.ID,
		Healthy:    *healthyCond,
		Conditions: current.Conditions,
	}, nil
}
