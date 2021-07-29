package stats

import "time"

type Aggregator struct {
	measures []measure
	sum      float64
}

type measure struct {
	timestamp time.Time
	value     float64
}

func (a *Aggregator) Add(ts time.Time, value float64) *Aggregator {
	a.sum += value
	insertIdx := len(a.measures)
	for insertIdx > 0 && ts.Before(a.measures[insertIdx-1].timestamp) {
		insertIdx--
	}
	a.measures = insertMeasure(a.measures, insertIdx, measure{ts, value})
	return a
}

func insertMeasure(slc []measure, idx int, val measure) []measure {
	slc = append(slc, measure{})
	copy(slc[idx+1:], slc[idx:])
	slc[idx] = val
	return slc
}

func (a *Aggregator) Clip(window time.Duration) *Aggregator {
	if len(a.measures) == 0 {
		return a
	}
	threshold := a.measures[len(a.measures)-1].timestamp.Add(-window)
	for len(a.measures) > 0 && !threshold.Before(a.measures[0].timestamp) {
		a.sum -= a.measures[0].value
		a.measures = a.measures[1:]
	}
	return a
}

func (a Aggregator) Average() float64 {
	if len(a.measures) == 0 {
		return 0
	}
	return a.sum / float64(len(a.measures))
}
