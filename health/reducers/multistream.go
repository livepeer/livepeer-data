package reducers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ConditionMultistreamHealthy health.ConditionType = "MultistreamHealthy"

	ConditionSingleMultistreamConnected health.ConditionType = "Connected"

	webhooksExchange      = "webhooks_default_exchange"
	multistreamBindingKey = "events.multistream.#"
)

type MultistreamReducer struct{}

func (t MultistreamReducer) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: webhooksExchange, Key: multistreamBindingKey}}
}

func (t MultistreamReducer) Conditions() []health.ConditionType {
	return []health.ConditionType{ConditionMultistreamHealthy}
}

func (t MultistreamReducer) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
	evt, ok := evtIface.(*data.WebhookEvent)
	if !ok {
		return current, nil
	}
	if !strings.HasPrefix(evt.Event, "multistream.") {
		return current, nil
	}

	ts := evt.Timestamp()
	var payload data.MultistreamWebhookPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		glog.Errorf("Error parsing multistream webhook payload. err=%q", err)
		return current, nil
	}
	target := payload.Target

	multistream, idx := cloneAppendMultistreamStatus(current.Multistream, target)
	conditions := multistream[idx].Conditions
	for i, cond := range conditions {
		if status := multistreamConditionStatus(evt, cond.Type); status != nil {
			conditions[i] = health.NewCondition(cond.Type, ts, status, nil, cond)
		}
	}
	rootConditions := make([]*health.Condition, len(current.Conditions))
	copy(rootConditions, current.Conditions)
	for i, cond := range rootConditions {
		if cond.Type == ConditionMultistreamHealthy {
			status := allTargetsConnected(multistream)
			conditions[i] = health.NewCondition(cond.Type, ts, &status, nil, cond)
		}
	}

	return &health.Status{
		ID:          current.ID,
		Healthy:     current.Healthy,
		Conditions:  rootConditions,
		Multistream: multistream,
	}, nil
}

var connectedCondByEvent = map[string]bool{
	"multistream.connected":    true,
	"multistream.disconnected": false,
	"multistream.error":        false,
}

func allTargetsConnected(multistream []*health.MultistreamStatus) bool {
	for _, ms := range multistream {
		for _, cond := range ms.Conditions {
			if cond.Type == ConditionSingleMultistreamConnected && (cond.Status == nil || !*cond.Status) {
				return false
			}
		}
	}
	return true
}

func multistreamConditionStatus(evt *data.WebhookEvent, condType health.ConditionType) *bool {
	switch condType {
	case ConditionSingleMultistreamConnected:
		connected, ok := connectedCondByEvent[evt.Event]
		if !ok {
			glog.Errorf("Unknown multistream webhook event. event=%q", evt.Event)
			return nil
		}
		return &connected
	default:
		glog.Errorf("Unknown multistream status condition. conditionType=%q", condType)
		return nil
	}
}

func cloneAppendMultistreamStatus(current []*health.MultistreamStatus, target data.MultistreamTargetInfo) ([]*health.MultistreamStatus, int) {
	multistream := make([]*health.MultistreamStatus, len(current))
	copy(multistream, current)
	for idx, ms := range multistream {
		if targetsEq(ms.Target, target) {
			multistream[idx] = cloneMultistreamStatus(ms)
			return multistream, idx
		}
	}

	multistream = append(multistream, &health.MultistreamStatus{
		Target: target,
		Conditions: []*health.Condition{
			health.NewCondition(ConditionSingleMultistreamConnected, time.Time{}, nil, nil, nil),
		},
	})
	return multistream, len(multistream) - 1
}

func cloneMultistreamStatus(status *health.MultistreamStatus) *health.MultistreamStatus {
	clone := *status
	clone.Conditions = make([]*health.Condition, len(status.Conditions))
	copy(clone.Conditions, status.Conditions)
	return &clone
}

func targetsEq(t1, t2 data.MultistreamTargetInfo) bool {
	return t1.Profile == t2.Profile && t1.ID == t2.ID
}
