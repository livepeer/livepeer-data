package views

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Masterminds/squirrel"
	"math"
	"strings"
	"time"
)

const maxClickhouseResultRows = 1000

type RealtimeViewershipRow struct {
	Timestamp   time.Time `ch:"timestamp_ts"`
	ViewCount   uint64    `ch:"view_count"`
	BufferRatio float64   `ch:"buffer_ratio"`
	ErrorRate   float64   `ch:"error_rate"`

	PlaybackID  string `ch:"playback_id"`
	DeviceType  string `ch:"device_type"`
	Browser     string `ch:"browser"`
	CountryName string `ch:"playback_country_name"`
}

type Clickhouse interface {
	QueryRealtimeViewsEvents(ctx context.Context, spec QuerySpec) ([]RealtimeViewershipRow, error)
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

func (c *ClickhouseClient) QueryRealtimeViewsEvents(ctx context.Context, spec QuerySpec) ([]RealtimeViewershipRow, error) {
	sql, args, err := buildRealtimeViewsEventsQuery(spec)
	if err != nil {
		return nil, fmt.Errorf("error building viewership events query: %w", err)
	}
	var res []RealtimeViewershipRow
	err = c.conn.Select(ctx, &res, sql, args...)
	if err != nil {
		return nil, err
	}
	res = replaceNaNBufferRatio(res)

	return res, nil
}

func buildRealtimeViewsEventsQuery(spec QuerySpec) (string, []interface{}, error) {
	var query squirrel.SelectBuilder
	if spec.From == nil && spec.To == nil {
		query = currentEventsQuery(spec)
	} else {
		query = timeRangeEventsQuery(spec)
	}

	query = withPlaybackIdFilter(query, spec.Filter.PlaybackID)
	if creatorId := spec.Filter.CreatorID; creatorId != "" {
		query = query.Where("creator_id = ?", creatorId)
	}

	for _, by := range spec.BreakdownBy {
		field, ok := realtimeViewershipBreakdownFields[by]
		if !ok {
			return "", nil, fmt.Errorf("invalid breakdown field: %s", by)
		}
		// skip breakdowns that are already in the query
		// only happens when playbackId or dStorageUrl is specified
		if sql, _, _ := query.ToSql(); strings.Contains(sql, field) {
			continue
		}
		query = query.Columns(field).GroupBy(field)
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

// currentEventsQuery uses an unoptimized raw playback logs to favor latency over query speed.
func currentEventsQuery(spec QuerySpec) squirrel.SelectBuilder {
	return squirrel.Select(
		"count(distinct session_id) as view_count").
		From("viewership_events").
		Where("user_id = ?", spec.Filter.UserID).
		Where("server_timestamp > (toUnixTimestamp(now() - 30)) * 1000").
		Limit(maxClickhouseResultRows)
}

// timeRangeEventsQuery uses an optimized materialized view "per minute" to favor query time against the latency.
func timeRangeEventsQuery(spec QuerySpec) squirrel.SelectBuilder {
	query := squirrel.Select(
		"timestamp_ts",
		"count(distinct session_id) as view_count",
		"sum(buffer_ms) / (sum(playtime_ms) + sum(buffer_ms)) as buffer_ratio",
		"sum(if(errors > 0, 1, 0)) / count(*) as error_rate").
		From("viewership_sessions_by_minute").
		Where("user_id = ?", spec.Filter.UserID).
		GroupBy("timestamp_ts").
		OrderBy("timestamp_ts desc").
		Limit(maxClickhouseResultRows)

	if spec.From != nil {
		// timestamp_ts is DateTime, but it's automatically converted to seconds
		query = query.Where("timestamp_ts >= ?", spec.From.UnixMilli()/1000)
	}
	if spec.To != nil {
		// timestamp_ts is DateTime, but it's automatically converted to seconds
		query = query.Where("timestamp_ts < ?", spec.To.UnixMilli()/1000)
	}
	return query
}

func replaceNaNBufferRatio(rows []RealtimeViewershipRow) []RealtimeViewershipRow {
	var res []RealtimeViewershipRow
	for _, r := range rows {
		if math.IsNaN(r.BufferRatio) {
			r.BufferRatio = 0.0
		}
		res = append(res, r)
	}
	return res
}
