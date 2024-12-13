package ai

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Masterminds/squirrel"
)

const maxClickhouseResultRows = 1000

type AIStreamStatusEventRow struct {
	StreamID      string   `ch:"stream_id"`
	AvgInputFPS   float64  `ch:"avg_input_fps"`
	AvgOutputFPS  float64  `ch:"avg_output_fps"`
	ErrorCount    uint64   `ch:"error_count"`
	Errors        []string `ch:"errors"`
	TotalRestarts uint64   `ch:"total_restarts"`
	RestartLogs   []string `ch:"restart_logs"`
}

type Clickhouse interface {
	QueryAIStreamStatusEvents(ctx context.Context, spec QuerySpec) ([]AIStreamStatusEventRow, error)
}

type ClickhouseOptions struct {
	Addr     string
	User     string
	Password string
	Database string
}

type ClickhouseClient struct {
	conn driver.Conn
}

func NewClickhouseConn(opts ClickhouseOptions) (*ClickhouseClient, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: strings.Split(opts.Addr, ","),
		Auth: clickhouse.Auth{
			Database: opts.Database,
			Username: opts.User,
			Password: opts.Password,
		},
		TLS: &tls.Config{},
	})
	if err != nil {
		return nil, err
	}
	return &ClickhouseClient{conn: conn}, nil
}

func (c *ClickhouseClient) QueryAIStreamStatusEvents(ctx context.Context, spec QuerySpec) ([]AIStreamStatusEventRow, error) {
	sql, args, err := buildAIStreamStatusEventsQuery(spec)
	if err != nil {
		return nil, fmt.Errorf("error building AI stream status events query: %w", err)
	}
	var res []AIStreamStatusEventRow
	err = c.conn.Select(ctx, &res, sql, args...)
	if err != nil {
		return nil, err
	}
	res = replaceNaN(res)
	return res, nil
}

func buildAIStreamStatusEventsQuery(spec QuerySpec) (string, []interface{}, error) {
	query := squirrel.Select(
		"stream_id",
		"avg(input_fps) as avg_input_fps",
		"avg(output_fps) as avg_output_fps",
		"countIf(last_error != '') as error_count",
		"arrayFilter(x -> x != '', groupUniqArray(last_error)) as errors",
		"sum(restart_count) as total_restarts",
		"arrayFilter(x -> x != '', groupUniqArray(last_restart_logs)) as restart_logs").
		From("stream_status").
		GroupBy("stream_id").
		Limit(maxClickhouseResultRows + 1)

	if spec.Filter.StreamID != "" {
		query = query.Where("stream_id = ?", spec.Filter.StreamID)
	}

	if spec.From != nil {
		query = query.Where("timestamp_ts > ?", spec.From)
	}

	if spec.To != nil {
		query = query.Where("timestamp_ts < ?", spec.To)
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

func replaceNaN(rows []AIStreamStatusEventRow) []AIStreamStatusEventRow {
	var res []AIStreamStatusEventRow
	for _, r := range rows {
		if math.IsNaN(r.AvgInputFPS) {
			r.AvgInputFPS = 0.0
		}
		if math.IsNaN(r.AvgOutputFPS) {
			r.AvgOutputFPS = 0.0
		}
		res = append(res, r)
	}
	return res
}
