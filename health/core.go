package health

import (
	"time"
)

var healthyMustHaves = map[ConditionType]bool{
	ConditionTranscoding: true,
	ConditionRealTime:    true,
}
var defaultConditions = []ConditionType{ConditionTranscoding, ConditionRealTime, ConditionNoErrors}
var statsWindows = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}

type Core struct {
	RecordStorage
}

func (c *Core) HandleEvent(evt Event) {
	mid := evt.ManifestID()
	record := c.GetOrCreate(mid, defaultConditions, statsWindows)

	record.LastStatus = reduceStreamHealth(record, evt)
	record.PastEvents = append(record.PastEvents, evt) // TODO: crop/drop these at some point
}

func reduceStreamHealth(record *Record, evt Event) Status {
	ts := evt.Timestamp()
	conditions := make([]*Condition, len(record.Conditions))
	for i, condType := range record.Conditions {
		status := conditionStatus(evt, condType)
		stats := record.ConditionStats[condType].Averages(record.StatsWindows, ts, status)

		last := record.LastStatus.GetCondition(condType)
		conditions[i] = NewCondition(condType, ts, status, stats, last)
	}

	return Status{
		ManifestID: evt.ManifestID(),
		Healthy:    diagnoseStream(record, conditions, ts),
		Conditions: conditions,
	}
}

func conditionStatus(evtIface Event, condType ConditionType) *bool {
	switch evt := evtIface.(type) {
	case *TranscodeEvent:
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
		}
	}
	return nil
}

func diagnoseStream(record *Record, currConditions []*Condition, ts time.Time) Condition {
	healthyMustsCount := 0
	for _, cond := range currConditions {
		if healthyMustHaves[cond.Type] && cond.Status != nil && *cond.Status {
			healthyMustsCount++
		}
	}
	isHealthy := healthyMustsCount == len(healthyMustHaves)
	healthStats := record.HealthStats.Averages(record.StatsWindows, ts, &isHealthy)

	last := &record.LastStatus.Healthy
	return *NewCondition("", ts, &isHealthy, healthStats, last)
}
