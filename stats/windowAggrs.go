package stats

import "time"

type WindowAggregators map[Window]*Aggregator

type ByWindow = map[Window]float64

func (a WindowAggregators) Averages(windows []time.Duration, ts time.Time, measure *float64) ByWindow {
	stats := ByWindow{}
	for _, windowDur := range windows {
		window := Window{windowDur}
		aggr, ok := a[window]
		if !ok {
			aggr = &Aggregator{}
			a[window] = aggr
		}
		if measure != nil {
			aggr.Add(ts, *measure)
		}
		stats[window] = aggr.Clip(windowDur).Average()
	}
	return stats
}
