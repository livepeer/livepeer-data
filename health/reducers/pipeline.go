package reducers

import (
	"time"

	"github.com/livepeer/livepeer-data/health"
)

var (
	statsWindows   = []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour}
	maxStatsWindow = statsWindows[len(statsWindows)-1]
)

func DefaultPipeline(golpExchange string, shardPrefixes []string) (reducers []health.Reducer, starTimeOffset time.Duration) {
	return []health.Reducer{
		TranscodeReducer{golpExchange, shardPrefixes},
		MultistreamReducer{},
		HealthReducer,
		StatsReducer(statsWindows),
	}, maxStatsWindow
}
