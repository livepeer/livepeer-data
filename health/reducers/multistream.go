package reducers

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	ConditionMultistreaming data.ConditionType = "Multistreaming"

	webhooksExchange      = "webhook_default_exchange"
	multistreamBindingKey = "events.multistream.#"
)

type MultistreamReducer struct{}

func (t MultistreamReducer) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: webhooksExchange, Key: multistreamBindingKey}}
}

func (t MultistreamReducer) Conditions() []data.ConditionType {
	return []data.ConditionType{ConditionMultistreaming}
}

func (t MultistreamReducer) Reduce(current *data.HealthStatus, _ interface{}, evtIface data.Event) (*data.HealthStatus, interface{}) {
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
	currConnected := multistream[idx].Connected

	switch evt.Event {
	case "multistream.connected":
		status := true
		multistream[idx] = &data.MultistreamStatus{
			Target:    target,
			Connected: data.NewCondition("", ts, &status, currConnected),
		}
	case "multistream.error":
		status := false
		multistream[idx] = &data.MultistreamStatus{
			Target:    target,
			Connected: data.NewCondition("", ts, &status, currConnected),
		}
	case "multistream.disconnected":
		multistream = append(multistream[:idx], multistream[idx+1:]...)
	default:
		glog.Errorf("Unknown multistream webhook event. event=%q", evt.Event)
	}

	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if cond.Type == ConditionMultistreaming {
			if len(multistream) == 0 {
				conditions[i] = &data.Condition{Type: ConditionMultistreaming}
			} else {
				status := allTargetsConnected(multistream)
				conditions[i] = data.NewCondition(cond.Type, ts, &status, cond)
			}
		}
	}

	return data.NewMergedHealthStatus(current, data.HealthStatus{
		Conditions:  conditions,
		Multistream: multistream,
	}), nil
}

func allTargetsConnected(multistream []*data.MultistreamStatus) bool {
	for _, ms := range multistream {
		if ms.Connected.Status == nil || !*ms.Connected.Status {
			return false
		}
	}
	return true
}

func findOrCreateMultistreamStatus(multistream []*data.MultistreamStatus, target data.MultistreamTargetInfo) ([]*data.MultistreamStatus, int) {
	for idx, ms := range multistream {
		if targetsEq(ms.Target, target) {
			return multistream, idx
		}
	}

	multistream = append(multistream, &data.MultistreamStatus{
		Target:    target,
		Connected: data.NewCondition("", time.Time{}, nil, nil),
	})
	return multistream, len(multistream) - 1
}

func targetsEq(t1, t2 data.MultistreamTargetInfo) bool {
	return t1.Profile == t2.Profile && t1.ID == t2.ID
}
