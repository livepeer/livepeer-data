package ai

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type MockClickhouseClient struct {
	rows []AIStreamStatusEventRow
}

func (m MockClickhouseClient) QueryAIStreamStatusEvents(ctx context.Context, spec QuerySpec) ([]AIStreamStatusEventRow, error) {
	return m.rows, nil
}

func TestQueryAIStreamStatusEvents(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name    string
		spec    QuerySpec
		rows    []AIStreamStatusEventRow
		expJson string
	}{
		{
			name: "basic query with no errors",
			rows: []AIStreamStatusEventRow{
				{
					StreamID:      "stream-1",
					AvgInputFPS:   30.0,
					AvgOutputFPS:  25.0,
					ErrorCount:    0,
					Errors:        []string{},
					TotalRestarts: 1,
					RestartLogs:   []string{"restart-log-1"},
				},
			},
			expJson: `
				[
					{
						"streamId": "stream-1",
						"avgInputFps": 30.0,
						"avgOutputFps": 25.0,
						"errorCount": 0,
						"errors": [],
						"totalRestarts": 1,
						"restartLogs": ["restart-log-1"]
					}
				]
			`,
		},
		{
			name: "query with errors",
			rows: []AIStreamStatusEventRow{
				{
					StreamID:      "stream-2",
					AvgInputFPS:   20.0,
					AvgOutputFPS:  15.0,
					ErrorCount:    2,
					Errors:        []string{"error-1", "error-2"},
					TotalRestarts: 3,
					RestartLogs:   []string{"restart-log-2", "restart-log-3"},
				},
			},
			expJson: `
				[
					{
						"streamId": "stream-2",
						"avgInputFps": 20.0,
						"avgOutputFps": 15.0,
						"errorCount": 2,
						"errors": ["error-1", "error-2"],
						"totalRestarts": 3,
						"restartLogs": ["restart-log-2", "restart-log-3"]
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
			res, err := client.QueryAIStreamStatusEvents(context.Background(), tt.spec)

			// then
			require.NoError(err)
			jsonRes, err := json.Marshal(res)
			require.NoError(err)
			require.JSONEq(tt.expJson, string(jsonRes))
		})
	}
}
