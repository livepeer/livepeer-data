package views

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Masterminds/squirrel"
	"strings"
	"time"
)

const maxBigClickhouseResultRows = 10000

type RealtimeViewershipRow struct {
	Timestamp     time.Time `ch:"timestamp_ts"`
	ViewCount     uint64    `ch:"view_count"`
	BufferRatio   float64   `ch:"buffer_ratio"`
	ErrorSessions uint64    `ch:"error_sessions"`

	PlaybackID  string `ch:"playback_id"`
	Device      string `ch:"device"`
	Browser     string `ch:"browser"`
	CountryName string `ch:"country_name"`
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
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
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
	return res, err
}

func buildRealtimeViewsEventsQuery(spec QuerySpec) (string, []interface{}, error) {
	query := squirrel.Select(
		"timestamp_ts",
		"count(*) as view_count",
		"sum(buffer_ms) / sum(playtime_ms) as buffer_ratio",
		"sum(if(errors > 0, 1, 0)) as error_sessions").
		From("viewership_sessions_by_minute").
		Where("user_id = ?", spec.Filter.UserID).
		// TODO: Set limit depending on the timeStep
		//Limit(maxBigClickhouseResultRows + 1).
		Limit(1).
		OrderBy("timestamp_ts desc")

	// TODO: Check if we need it
	//query = withPlaybackIdFilter(query, spec.Filter.PlaybackID)
	//
	//if creatorId := spec.Filter.CreatorID; creatorId != "" {
	//	query = query.Where("creator_id_type = ?", "unverified")
	//	query = query.Where("creator_id = ?", creatorId)
	//}

	// TODO: Accept from and to
	//if from := spec.From; from != nil {
	//	query = query.Where("time >= timestamp_millis(?)", from.UnixMilli())
	//}
	//if to := spec.To; to != nil {
	//	query = query.Where("time < timestamp_millis(?)", to.UnixMilli())
	//}

	// TODO: Accept breakdown
	//for _, by := range spec.BreakdownBy {
	//	field, ok := viewershipBreakdownFields[by]
	//	if !ok {
	//		return "", nil, fmt.Errorf("invalid breakdown field: %s", by)
	//	}
	//	// skip breakdowns that are already in the query
	//	// only happens when playbackId or dStorageUrl is specified
	//	if sql, _, _ := query.ToSql(); strings.Contains(sql, field) {
	//		continue
	//	}
	//	query = query.Columns(field).GroupBy(field)
	//}

	query = query.Where("timestamp_ts >= now() - INTERVAL 30 MINUTE")
	query = query.Columns("timestamp_ts").GroupBy("timestamp_ts")

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}
