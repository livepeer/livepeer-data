package health

import (
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const ConditionActive data.ConditionType = "Active"

type Reducer interface {
	Bindings() []event.BindingArgs
	Conditions() []data.ConditionType
	Reduce(current *data.HealthStatus, state interface{}, evt data.Event) (*data.HealthStatus, interface{})
}

type ReducerFunc func(*data.HealthStatus, interface{}, data.Event) (*data.HealthStatus, interface{})

func (f ReducerFunc) Bindings() []event.BindingArgs    { return nil }
func (f ReducerFunc) Conditions() []data.ConditionType { return nil }
func (f ReducerFunc) Reduce(current *data.HealthStatus, state interface{}, evt data.Event) (*data.HealthStatus, interface{}) {
	return f(current, state, evt)
}
