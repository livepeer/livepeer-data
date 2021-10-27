package reducers

import (
	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

type Pipeline []health.Reducer

func (p Pipeline) Bindings() []event.BindingArgs {
	added := map[string]bool{}
	bindings := []event.BindingArgs{}
	for _, reducer := range p {
		for _, newBind := range reducer.Bindings() {
			key := newBind.Key + " / " + newBind.Exchange
			if added[key] {
				glog.Errorf("Ignoring duplicate binding in pipeline. key=%q exchange=%q args=%+v", newBind.Key, newBind.Exchange, newBind.Args)
				continue
			}
			added[key] = true
			bindings = append(bindings, newBind)
		}
	}
	return bindings
}

func (p Pipeline) Conditions() []health.ConditionType {
	added := map[health.ConditionType]bool{}
	conds := []health.ConditionType{}
	for _, reducer := range p {
		for _, newCond := range reducer.Conditions() {
			if added[newCond] {
				glog.Errorf("Ignoring duplicate condition in pipeline. condition=%q", newCond)
				continue
			}
			added[newCond] = true
			conds = append(conds, newCond)
		}
	}
	return conds
}

func (p Pipeline) Reduce(current *health.Status, stateIface interface{}, evt data.Event) (*health.Status, interface{}) {
	var state []interface{}
	if stateIface != nil {
		state = stateIface.([]interface{})
	} else {
		state = make([]interface{}, len(p))
	}
	for i, reducer := range p {
		current, state[i] = reducer.Reduce(current, state[i], evt)
	}
	return current, state
}
