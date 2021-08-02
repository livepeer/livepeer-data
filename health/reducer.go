package health

import (
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

type Reducer interface {
	Bindings() []event.BindingArgs
	Conditions() []ConditionType
	Reduce(current Status, state interface{}, evt data.Event) (Status, interface{})
}

type ReducerFunc func(Status, interface{}, data.Event) (Status, interface{})

func (f ReducerFunc) Bindings() []event.BindingArgs { return nil }
func (f ReducerFunc) Conditions() []ConditionType   { return nil }
func (f ReducerFunc) Reduce(current Status, state interface{}, evt data.Event) (Status, interface{}) {
	return f(current, state, evt)
}
