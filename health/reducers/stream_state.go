package reducers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	streamStateBindingKey = "stream.state.#"
)

type ActiveConditionExtraData struct {
	NodeID string `json:"nodeId"`
	Region string `json:"region"`
}

type StreamStateReducer struct {
	exchange string
}

func (t StreamStateReducer) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: t.exchange, Key: streamStateBindingKey}}
}

func (t StreamStateReducer) Conditions() []data.ConditionType {
	return []data.ConditionType{health.ConditionActive}
}

func (t StreamStateReducer) Reduce(current *data.HealthStatus, _ interface{}, evtIface data.Event) (*data.HealthStatus, interface{}) {
	evt, ok := evtIface.(*data.StreamStateEvent)
	if !ok {
		return current, nil
	}
	glog.Infof("Stream state event: region=%s stream=%s active=%v", evt.Region, evt.StreamID(), evt.State.Active)

	isActive := evt.State.Active
	last := GetLastActiveData(current)
	hasLast := last.NodeID != "" && last.Region != ""
	if !isActive && hasLast && (evt.NodeID != last.NodeID || evt.Region != last.Region) {
		glog.Infof("Ignoring inactive stream state event from previous session: region=%s stream=%s", evt.Region, evt.StreamID())
		return current, nil
	}

	conditions := current.ConditionsCopy()
	if isActive {
		// Clear all previous state when the stream becomes active.
		// TODO: We should actually make the stream state indexed by the session ID
		// not to need this. Would need to add session ID in all event payloads.
		conditions = clearConditions(conditions)
		current = data.NewHealthStatus(current.ID, conditions)
	}
	for i, cond := range conditions {
		if cond.Type == health.ConditionActive {
			newCond := data.NewCondition(cond.Type, evt.Timestamp(), &isActive, cond)
			newCond.ExtraData = ActiveConditionExtraData{NodeID: evt.NodeID, Region: evt.Region}
			conditions[i] = newCond
		}
	}
	return data.NewMergedHealthStatus(current, data.HealthStatus{
		Conditions: conditions,
	}), nil
}

func clearConditions(conditions []*data.Condition) []*data.Condition {
	cleared := make([]*data.Condition, len(conditions))
	for i, cond := range conditions {
		cleared[i] = data.NewCondition(cond.Type, time.Time{}, nil, nil)
	}
	return cleared
}

func GetLastActiveData(status *data.HealthStatus) ActiveConditionExtraData {
	data, ok := status.Condition(health.ConditionActive).ExtraData.(ActiveConditionExtraData)
	if !ok {
		return ActiveConditionExtraData{}
	}
	return data
}
