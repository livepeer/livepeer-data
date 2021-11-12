package reducers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	globalExchange        = "lp_global_replication"
	streamStateBindingKey = "stream.state.#"

	ConditionActive health.ConditionType = "Active"
)

type ActiveConditionExtraData struct {
	NodeID string `json:"nodeId"`
	Region string `json:"region"`
}

type StreamStateReducer struct{}

func (t StreamStateReducer) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: globalExchange, Key: streamStateBindingKey}}
}

func (t StreamStateReducer) Conditions() []health.ConditionType {
	return []health.ConditionType{ConditionActive}
}

func (t StreamStateReducer) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
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
		current = health.NewStatus(current.ID, conditions)
	}
	for i, cond := range conditions {
		if cond.Type == ConditionActive {
			newCond := health.NewCondition(cond.Type, evt.Timestamp(), &isActive, cond)
			newCond.ExtraData = ActiveConditionExtraData{NodeID: evt.NodeID, Region: evt.Region}
			conditions[i] = newCond
		}
	}
	return health.NewMergedStatus(current, health.Status{
		Conditions: conditions,
	}), nil
}

func clearConditions(conditions []*health.Condition) []*health.Condition {
	cleared := make([]*health.Condition, len(conditions))
	for i, cond := range conditions {
		cleared[i] = health.NewCondition(cond.Type, time.Time{}, nil, nil)
	}
	return cleared
}

func GetLastActiveData(status *health.Status) ActiveConditionExtraData {
	data, ok := status.Condition(ConditionActive).ExtraData.(ActiveConditionExtraData)
	if !ok {
		return ActiveConditionExtraData{}
	}
	return data
}
