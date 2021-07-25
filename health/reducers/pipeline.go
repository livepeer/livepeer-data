package reducers

import (
	"time"

	"github.com/livepeer/healthy-streams/health"
)

var (
	statsWindows   = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}
	maxStatsWindow = statsWindows[len(statsWindows)-1]
)

func DefaultPipeline() (reducers []health.Reducer, starTimeOffset time.Duration) {
	return []health.Reducer{
		TranscodeReducer{},
		HealthReducer,
		StatsReducer(statsWindows),
	}, maxStatsWindow
}
