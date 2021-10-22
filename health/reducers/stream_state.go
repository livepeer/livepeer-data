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

type StreamStateMetrics struct{}

func (t StreamStateMetrics) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: globalExchange, Key: streamStateBindingKey}}
}

func (t StreamStateMetrics) Conditions() []health.ConditionType {
	return []health.ConditionType{ConditionActive}
}

func (t StreamStateMetrics) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
	evt, ok := evtIface.(*data.StreamStateEvent)
	if !ok {
		return current, nil
	}
	glog.Infof("Stream state event: region=%s stream=%s active=%v", evt.Region, evt.StreamID(), evt.State.Active)

	lastActiveRegion := current.LastActiveRegion
	if evt.State.Active {
		lastActiveRegion = evt.Region
	} else if lastActiveRegion != "" && evt.Region != lastActiveRegion {
		glog.Infof("Ignoring inactive stream state event from previous session: region=%s stream=%s", evt.Region, evt.StreamID())
		return current, nil
	}

	conditions := current.ConditionsCopy()
	for i, cond := range conditions {
		if cond.Type == ConditionActive {
			status := evt.State.Active
			conditions[i] = health.NewCondition(cond.Type, evt.Timestamp(), &status, nil, cond)
		}
	}
	return health.NewMergedStatus(current, health.Status{
		LastActiveRegion: lastActiveRegion,
		Conditions:       conditions,
	}), nil
}
