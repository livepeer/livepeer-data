package reducers

import (
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

	last := GetLastActiveData(current)
	if !evt.State.Active && (evt.NodeID != last.NodeID || evt.Region != last.Region) {
		glog.Infof("Ignoring inactive stream state event from previous session: region=%s stream=%s", evt.Region, evt.StreamID())
		return current, nil
	}

	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if cond.Type == ConditionActive {
			status := evt.State.Active
			newCond := health.NewCondition(cond.Type, evt.Timestamp(), &status, cond)
			newCond.ExtraData = ActiveConditionExtraData{NodeID: evt.NodeID, Region: evt.Region}
			conditions[i] = newCond
		}
	}
	return health.NewMergedStatus(current, health.Status{
		Conditions: conditions,
	}), nil
}

func GetLastActiveData(status *health.Status) ActiveConditionExtraData {
	data, ok := status.Condition(ConditionActive).ExtraData.(ActiveConditionExtraData)
	if !ok {
		return ActiveConditionExtraData{}
	}
	return data
}
