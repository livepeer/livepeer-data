package health

import (
	"fmt"
	"time"

	"github.com/livepeer/healthy-streams/event"
)

func HandleTranscodeEvent(record *Record, evtIface Event) (Status, bool) {
	evt, ok := evtIface.(*TranscodeEvent)
	if !ok {
		return Status{}, false
	}

	ts := evt.Timestamp()
	conditions := make([]*Condition, len(record.Conditions))
	for i, condType := range record.Conditions {
		status := conditionStatus(evt, condType)
		stats := record.ConditionStats[condType].Averages(record.StatsWindows, ts, ptrBoolToFloat(status))

		last := record.LastStatus.GetCondition(condType)
		conditions[i] = NewCondition(condType, ts, status, stats, last)
	}

	return Status{
		ManifestID: evt.ManifestID(),
		Healthy:    diagnoseStream(record, conditions, ts),
		Conditions: conditions,
	}, true
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
	healthStats := record.HealthStats.Averages(record.StatsWindows, ts, ptrBoolToFloat(&isHealthy))

	last := &record.LastStatus.Healthy
	return *NewCondition("", ts, &isHealthy, healthStats, last)
}

func consumeOptions(opts StreamingOptions) (event.ConsumeOptions, error) {
	streamOpts, err := event.ParseStreamOptions(opts.RawStreamOptions)
	if err != nil {
		return event.ConsumeOptions{}, fmt.Errorf("bad stream options: %w", err)
	}

	startTime := time.Now().Add(-maxStatsWindow)
	return event.ConsumeOptions{
		Stream: opts.Stream,
		StreamOptions: &event.StreamOptions{
			StreamOptions: *streamOpts,
			Bindings: []event.BindingArgs{
				{Key: bindingKey, Exchange: opts.Exchange},
			},
		},
		ConsumerOptions: event.NewConsumerOptions(opts.ConsumerName, event.TimestampOffset(startTime)),
		MemorizeOffset:  true,
	}, nil
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
