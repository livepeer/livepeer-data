package stats

import "time"

type WindowAggregators map[Window]*Aggregator

type ByWindow = map[Window]float64

func (a WindowAggregators) Averages(windows []time.Duration, ts time.Time, status *bool) ByWindow {
	measure := float64(0)
	if status != nil && *status {
		measure = 1
	}

	stats := ByWindow{}
	for _, windowDur := range windows {
		window := Window{windowDur}
		aggr, ok := a[window]
		if !ok {
			aggr = &Aggregator{}
			a[window] = aggr
		}
		if status != nil {
			aggr.Add(ts, measure)
		}
		stats[window] = aggr.Clip(windowDur).Average()
	}
	return stats
}
