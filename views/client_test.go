package views

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var timestamp = time.UnixMilli(1646555400000)

type MockClickhouseClient struct {
	rows []RealtimeViewershipRow
}

func (m MockClickhouseClient) QueryRealtimeViewsEvents(ctx context.Context, spec QuerySpec) ([]RealtimeViewershipRow, error) {
	return m.rows, nil
}

func TestQueryRealtimeEvents(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name    string
		spec    QuerySpec
		rows    []RealtimeViewershipRow
		expJson string
	}{
		{
			name: "current with no breakdown",
			rows: []RealtimeViewershipRow{
				{
					ViewCount: 2,
				},
			},
			expJson: `
				[
					{
						"viewCount":2
					}
				]
			`,
		},
		{
			name: "current with breakdown by playbackId and country",
			spec: QuerySpec{
				BreakdownBy: []string{"playbackId", "country"},
			},
			rows: []RealtimeViewershipRow{
				{
					ViewCount:   2,
					PlaybackID:  "playbackid-1",
					CountryName: "Poland",
				},
			},
			expJson: `
				[
					{
						"viewCount": 2,
						"playbackId": "playbackid-1",
						"country": "Poland"
					}
				]
			`,
		},
		{
			name: "time range with no breakdown",
			spec: QuerySpec{From: &timestamp},
			rows: []RealtimeViewershipRow{
				{
					Timestamp:   timestamp,
					ViewCount:   2,
					BufferRatio: 0.5,
					ErrorRate:   0.25,
				},
			},
			expJson: `
				[
					{
						"timestamp":1646555400000,
						"viewCount":2,
						"rebufferRatio":0.5,
						"errorRate":0.25
					}
				]
			`,
		},
		{
			name: "time range with breakdown by playbackId and country",
			spec: QuerySpec{
				From:        &timestamp,
				BreakdownBy: []string{"playbackId", "country"},
			},
			rows: []RealtimeViewershipRow{
				{
					Timestamp:   timestamp,
					ViewCount:   2,
					BufferRatio: 0.5,
					ErrorRate:   0.25,
					PlaybackID:  "playbackid-1",
					CountryName: "Poland",
				},
			},
			expJson: `
				[
					{
						"timestamp": 1646555400000,
						"viewCount": 2,
						"rebufferRatio": 0.5,
						"errorRate": 0.25,
						"playbackId": "playbackid-1",
						"country": "Poland"
					}
				]
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			mockClickhouse := MockClickhouseClient{rows: tt.rows}
			client := Client{clickhouse: &mockClickhouse}

			// when
			res, err := client.QueryRealtimeEvents(context.Background(), tt.spec)

			// then
			require.NoError(err)
			jsonRes, err := json.Marshal(res)
			require.NoError(err)
			require.JSONEq(tt.expJson, string(jsonRes))
		})
	}
}
