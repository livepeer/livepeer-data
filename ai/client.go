package ai

import (
	"context"
	"errors"
	"fmt"

	livepeer "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	promClient "github.com/prometheus/client_golang/api"
)

var ErrAssetNotFound = errors.New("asset not found")

type StreamStatus struct {
	StreamID      string                 `json:"streamId"`
	AvgInputFPS   data.Nullable[float64] `json:"avgInputFps"`
	AvgOutputFPS  data.Nullable[float64] `json:"avgOutputFps"`
	ErrorCount    uint64                 `json:"errorCount"`
	Errors        []string               `json:"errors"`
	TotalRestarts uint64                 `json:"totalRestarts"`
	RestartLogs   []string               `json:"restartLogs"`
}

type ClientOptions struct {
	Prometheus promClient.Config
	Livepeer   livepeer.ClientOptions
	ClickhouseOptions
}

type Client struct {
	opts       ClientOptions
	lp         *livepeer.Client
	clickhouse Clickhouse
}

func NewClient(opts ClientOptions) (*Client, error) {
	lp := livepeer.NewAPIClient(opts.Livepeer)

	clickhouse, err := NewClickhouseConn(opts.ClickhouseOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating clickhouse client: %w", err)
	}

	return &Client{opts, lp, clickhouse}, nil
}

func (c *Client) QueryAIStreamStatusEvents(ctx context.Context, spec QuerySpec) ([]StreamStatus, error) {
	rows, err := c.clickhouse.QueryAIStreamStatusEvents(ctx, spec)
	if err != nil {
		return nil, err
	}
	metrics := aiStreamStatusEventsToStreamStatuses(rows, spec)
	return metrics, nil
}

func aiStreamStatusEventsToStreamStatuses(rows []AIStreamStatusEventRow, spec QuerySpec) []StreamStatus {
	streamStatuses := make([]StreamStatus, len(rows))
	for i, row := range rows {
		streamStatuses[i] = StreamStatus{
			StreamID:      row.StreamID,
			AvgInputFPS:   data.WrapNullable(row.AvgInputFPS),
			AvgOutputFPS:  data.WrapNullable(row.AvgOutputFPS),
			ErrorCount:    row.ErrorCount,
			Errors:        row.Errors,
			TotalRestarts: row.TotalRestarts,
			RestartLogs:   row.RestartLogs,
		}
	}
	return streamStatuses
}

func toFloat64Ptr(f float64, asked bool) data.Nullable[float64] {
	return data.ToNullable(f, true, asked)
}

func toStringPtr(s string, asked bool) data.Nullable[string] {
	return data.ToNullable(s, true, asked)
}
