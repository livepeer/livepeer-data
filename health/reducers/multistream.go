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

	multistream := current.MultistreamCopy()
	multistream, idx := findOrCreateMultistreamStatus(multistream, target)
	if status := connectedStatusFromEvent(evt); status != nil {
		currConnected := multistream[idx].Connected
		multistream[idx] = &health.MultistreamStatus{
			Target:    target,
			Connected: health.NewCondition("", ts, status, nil, currConnected),
		}
	}

	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if cond.Type == ConditionMultistreamHealthy {
			status := allTargetsConnected(multistream)
			conditions[i] = health.NewCondition(cond.Type, ts, &status, nil, cond)
		}
	}

	return health.NewMergedStatus(current, health.Status{
		Conditions:  conditions,
		Multistream: multistream,
	}), nil
}

func allTargetsConnected(multistream []*health.MultistreamStatus) bool {
	for _, ms := range multistream {
		if ms.Connected.Status == nil || !*ms.Connected.Status {
			return false
		}
	}
	return true
}

func connectedStatusFromEvent(evt *data.WebhookEvent) *bool {
	var connected bool
	switch evt.Event {
	case "multistream.connected":
		connected = true
	case "multistream.disconnected", "multistream.error":
		connected = false
	default:
		glog.Errorf("Unknown multistream webhook event. event=%q", evt.Event)
		return nil
	}
	return &connected
}

func findOrCreateMultistreamStatus(multistream []*health.MultistreamStatus, target data.MultistreamTargetInfo) ([]*health.MultistreamStatus, int) {
	for idx, ms := range multistream {
		if targetsEq(ms.Target, target) {
			return multistream, idx
		}
	}

	multistream = append(multistream, &health.MultistreamStatus{
		Target:    target,
		Connected: health.NewCondition("", time.Time{}, nil, nil, nil),
	})
	return multistream, len(multistream) - 1
}

func targetsEq(t1, t2 data.MultistreamTargetInfo) bool {
	return t1.Profile == t2.Profile && t1.ID == t2.ID
}
