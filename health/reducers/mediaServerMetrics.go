package reducers

import (
	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const (
	// ConditionMultistreaming health.ConditionType = "Multistreaming"

	mediaServerExchange = "lp_mist_api_connector"
	metricsBindingKey   = "stream_metrics.#"
)

type MediaServerMetrics struct{}

func (t MediaServerMetrics) Bindings() []event.BindingArgs {
	return []event.BindingArgs{{Exchange: mediaServerExchange, Key: metricsBindingKey}}
}

func (t MediaServerMetrics) Conditions() []health.ConditionType {
	// return []health.ConditionType{ConditionMultistreaming}
	return nil
}

func (t MediaServerMetrics) Reduce(current *health.Status, _ interface{}, evtIface data.Event) (*health.Status, interface{}) {
	evt, ok := evtIface.(*data.MediaServerMetricsEvent)
	if !ok {
		return current, nil
	}

	if evt.Stats.ViewerCount > 10 {
		glog.Warning("High viewer count in a stream! streamId=%q", evt.StreamID())
	}

	return health.NewMergedStatus(current, health.Status{}), nil
}
