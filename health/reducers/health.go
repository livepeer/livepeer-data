package reducers

import "github.com/livepeer/healthy-streams/health"

var healthyMustHaves = map[health.ConditionType]bool{
	ConditionTranscoding: true,
	ConditionRealTime:    true,
}

var HealthReducer = health.ReducerFunc(reduceHealth)

func reduceHealth(current health.Status, _ interface{}, evt health.Event) (health.Status, interface{}) {
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
