package views

import (
	require2 "github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestFilterMostRecent(t *testing.T) {
	require := require2.New(t)

	// given
	allRows := []RealtimeViewershipRow{
		{
			Timestamp:   time.UnixMilli(1646555400000),
			ViewCount:   5,
			BufferRatio: 0.1,
			ErrorRate:   0.2,
			PlaybackID:  "playbackid-1",
		},
		{
			Timestamp:   time.UnixMilli(1646555400000),
			ViewCount:   200,
			BufferRatio: 0.3,
			ErrorRate:   0.4,
			PlaybackID:  "playbackid-2",
		},
		{
			Timestamp:   time.UnixMilli(1646555000000),
			ViewCount:   3,
			BufferRatio: 0.2,
			ErrorRate:   0.3,
			PlaybackID:  "playbackid-1",
		},
		{
			Timestamp:   time.UnixMilli(1646555000000),
			ViewCount:   250,
			BufferRatio: 0.5,
			ErrorRate:   0.6,
			PlaybackID:  "playbackid-2",
		},
	}

	// when
	filtered := filterMostRecent(allRows)

	// then
	expFiltered := []RealtimeViewershipRow{
		{
			Timestamp:   time.UnixMilli(1646555400000),
			ViewCount:   5,
			BufferRatio: 0.1,
			ErrorRate:   0.2,
			PlaybackID:  "playbackid-1",
		},
		{
			Timestamp:   time.UnixMilli(1646555400000),
			ViewCount:   200,
			BufferRatio: 0.3,
			ErrorRate:   0.4,
			PlaybackID:  "playbackid-2",
		},
	}
	require.Equal(expFiltered, filtered)
}
