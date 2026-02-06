package usage

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Masterminds/squirrel"
)

type ClickhouseUsageSummaryRow struct {
	UserID       string     `ch:"user_id"`
	CreatorID    *string    `ch:"creator_id"`
	TimeInterval time.Time  `ch:"time_interval"`

	DeliveryUsageMins *float64 `ch:"delivery_usage_mins"`
	TotalUsageMins    *float64 `ch:"transcode_total_usage_mins"`
	StorageUsageMins  *float64 `ch:"storage_usage_mins"`
}

type ClickhouseActiveUsersSummaryRow struct {
	UserID string    `ch:"user_id"`
	Email  string    `ch:"email"`
	From   time.Time `ch:"interval_start_date"`
	To     time.Time `ch:"interval_end_date"`

	DeliveryUsageMins float64 `ch:"delivery_usage_mins"`
	TotalUsageMins    float64 `ch:"transcode_total_usage_mins"`
	StorageUsageMins  float64 `ch:"storage_usage_mins"`
}

type ClickhouseTotalUsageSummaryRow struct {
	DateTs                time.Time `ch:"date_ts"`
	DateS                 int64     `ch:"date_s"`
	WeekTs                time.Time `ch:"week_ts"`
	WeekS                 int64     `ch:"week_s"`
	VolumeEth             float64   `ch:"volume_eth"`
	VolumeUsd             float64   `ch:"volume_usd"`
	FeeDerivedMinutes     float64   `ch:"fee_derived_minutes"`
	ParticipationRate     float64   `ch:"participation_rate"`
	Inflation             float64   `ch:"inflation"`
	ActiveTranscoderCount int64     `ch:"active_transcoder_count"`
	DelegatorsCount       int64     `ch:"delegators_count"`
	AveragePricePerPixel  float64   `ch:"average_price_per_pixel"`
	AveragePixelPerMinute float64   `ch:"average_pixel_per_minute"`
}

const maxClickhouseResultRows = 10000

type ClickhouseOptions struct {
	Addr     string
	User     string
	Password string
	Database string

	HourlyUsageTable string
	DailyUsageTable  string
	UsersTable       string
}

type clickhouseHandler struct {
	opts ClickhouseOptions
	conn chdriver.Conn
}

func NewClickhouse(opts ClickhouseOptions) (BigQuery, error) {
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
		return nil, fmt.Errorf("error creating clickhouse connection: %w", err)
	}
	return &clickhouseHandler{opts: opts, conn: conn}, nil
}

func (ch *clickhouseHandler) QueryUsageSummary(ctx context.Context, spec QuerySpec) (*UsageSummaryRow, error) {
	if spec.HasAnyBreakdown() {
		return nil, fmt.Errorf("call QueryUsageSummaryWithBreakdown for breakdownBy or timeStep support")
	}

	sql, args, err := buildClickhouseUsageSummaryQuery(ch.opts.HourlyUsageTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}

	var rows []ClickhouseUsageSummaryRow
	err = ch.conn.Select(ctx, &rows, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse error: %w", err)
	} else if len(rows) > 1 {
		return nil, fmt.Errorf("internal error, query returned %d rows", len(rows))
	}

	if len(rows) == 0 {
		zero := float64(0)
		return &UsageSummaryRow{
			UserID:            spec.Filter.UserID,
			CreatorID:         toBqNullString(spec.Filter.CreatorID),
			DeliveryUsageMins: toBqNullFloat64(&zero),
			TotalUsageMins:    toBqNullFloat64(&zero),
			StorageUsageMins:  toBqNullFloat64(&zero),
		}, nil
	}

	return chUsageSummaryToBqRow(&rows[0]), nil
}

func (ch *clickhouseHandler) QueryUsageSummaryWithBreakdown(ctx context.Context, spec QuerySpec) ([]UsageSummaryRow, error) {
	sql, args, err := buildClickhouseUsageSummaryQuery(ch.opts.HourlyUsageTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}

	var rows []ClickhouseUsageSummaryRow
	err = ch.conn.Select(ctx, &rows, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse error: %w", err)
	} else if len(rows) > maxClickhouseResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxClickhouseResultRows)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	result := make([]UsageSummaryRow, len(rows))
	for i := range rows {
		result[i] = *chUsageSummaryToBqRow(&rows[i])
	}
	return result, nil
}

func (ch *clickhouseHandler) QueryTotalUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]TotalUsageSummaryRow, error) {
	sql, args, err := buildClickhouseTotalUsageSummaryQuery(ch.opts.DailyUsageTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building total usage summary query: %w", err)
	}

	var rows []ClickhouseTotalUsageSummaryRow
	err = ch.conn.Select(ctx, &rows, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse error: %w", err)
	} else if len(rows) > maxClickhouseResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxClickhouseResultRows)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	result := make([]TotalUsageSummaryRow, len(rows))
	for i, row := range rows {
		result[i] = TotalUsageSummaryRow{
			DateTs:                row.DateTs,
			DateS:                 row.DateS,
			WeekTs:                row.WeekTs,
			WeekS:                 row.WeekS,
			VolumeEth:             row.VolumeEth,
			VolumeUsd:             row.VolumeUsd,
			FeeDerivedMinutes:     row.FeeDerivedMinutes,
			ParticipationRate:     row.ParticipationRate,
			Inflation:             row.Inflation,
			ActiveTranscoderCount: row.ActiveTranscoderCount,
			DelegatorsCount:       row.DelegatorsCount,
			AveragePricePerPixel:  row.AveragePricePerPixel,
			AveragePixelPerMinute: row.AveragePixelPerMinute,
		}
	}
	return result, nil
}

func (ch *clickhouseHandler) QueryActiveUsersUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]ActiveUsersSummaryRow, error) {
	sql, args, err := buildClickhouseActiveUsersQuery(ch.opts.HourlyUsageTable, ch.opts.UsersTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building active users summary query: %w", err)
	}

	var rows []ClickhouseActiveUsersSummaryRow
	err = ch.conn.Select(ctx, &rows, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse error: %w", err)
	} else if len(rows) > maxClickhouseResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxClickhouseResultRows)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	result := make([]ActiveUsersSummaryRow, len(rows))
	for i, row := range rows {
		result[i] = ActiveUsersSummaryRow{
			UserID:            row.UserID,
			Email:             row.Email,
			From:              row.From,
			To:                row.To,
			DeliveryUsageMins: row.DeliveryUsageMins,
			TotalUsageMins:    row.TotalUsageMins,
			StorageUsageMins:  row.StorageUsageMins,
		}
	}
	return result, nil
}

// Query builders

var clickhouseTimeStepFuncs = map[string]string{
	"hour": "toStartOfHour",
	"day":  "toStartOfDay",
}

func buildClickhouseUsageSummaryQuery(table string, spec QuerySpec) (string, []interface{}, error) {
	if spec.Filter.UserID == "" {
		return "", nil, fmt.Errorf("userID cannot be empty")
	}

	query := squirrel.Select(
		"toFloat64(sum(transcode_total_usage_mins)) as transcode_total_usage_mins",
		"toFloat64(sum(delivery_usage_mins)) as delivery_usage_mins",
		"toFloat64(sum(storage_usage_mins) / count(distinct usage_hour_ts)) as storage_usage_mins").
		From(table).
		Limit(maxClickhouseResultRows + 1)

	if creatorID := spec.Filter.CreatorID; creatorID != "" {
		query = query.Columns("creator_id").
			Where("creator_id = ?", creatorID).
			GroupBy("creator_id")
	}

	if from := spec.From; from != nil {
		query = query.Where("usage_hour_ts >= fromUnixTimestamp64Milli(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("usage_hour_ts < fromUnixTimestamp64Milli(?)", to.UnixMilli())
	}

	if timeStep := spec.TimeStep; timeStep != "" {
		fn, ok := clickhouseTimeStepFuncs[timeStep]
		if !ok {
			return "", nil, fmt.Errorf("invalid time step: %s", timeStep)
		}
		query = query.
			Columns(fmt.Sprintf("%s(usage_hour_ts) as time_interval", fn)).
			GroupBy("time_interval").
			OrderBy("time_interval")
	}

	query = withClickhouseUserIdFilter(query, spec.Filter.UserID)

	for _, by := range spec.BreakdownBy {
		field, ok := usageBreakdownFields[by]
		if !ok {
			return "", nil, fmt.Errorf("invalid breakdown field: %s", by)
		}
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

func buildClickhouseTotalUsageSummaryQuery(table string, spec FromToQuerySpec) (string, []interface{}, error) {
	query := squirrel.Select(
		"date_ts", "date_s", "week_ts", "week_s",
		"volume_eth", "volume_usd", "fee_derived_minutes",
		"participation_rate", "inflation",
		"active_transcoder_count", "delegators_count",
		"average_price_per_pixel", "average_pixel_per_minute").
		From(table).
		Limit(maxClickhouseResultRows + 1).
		OrderBy("date_ts DESC")

	if from := spec.From; from != nil {
		query = query.Where("date_ts >= fromUnixTimestamp64Milli(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("date_ts < fromUnixTimestamp64Milli(?)", to.UnixMilli())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}
	return sql, args, nil
}

func buildClickhouseActiveUsersQuery(billingTable, usersTable string, spec FromToQuerySpec) (string, []interface{}, error) {
	query := squirrel.
		Select(
			"b.user_id",
			"u.email",
			"min(usage_hour_ts) as interval_start_date",
			"max(usage_hour_ts) as interval_end_date",
			"toFloat64(sum(transcode_total_usage_mins)) as transcode_total_usage_mins",
			"toFloat64(sum(delivery_usage_mins)) as delivery_usage_mins",
			"toFloat64(sum(storage_usage_mins) / count(distinct usage_hour_ts)) as storage_usage_mins",
		).
		From(fmt.Sprintf("%s as b", billingTable)).
		Join(fmt.Sprintf("%s as u on b.user_id = u.user_id", usersTable)).
		Where("not internal").
		GroupBy("b.user_id", "u.email").
		Having("transcode_total_usage_mins > 0 or delivery_usage_mins > 0 or storage_usage_mins > 0")

	if from := spec.From; from != nil {
		query = query.Where("usage_hour_ts >= fromUnixTimestamp64Milli(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("usage_hour_ts < fromUnixTimestamp64Milli(?)", to.UnixMilli())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}
	return sql, args, nil
}

func withClickhouseUserIdFilter(query squirrel.SelectBuilder, userID string) squirrel.SelectBuilder {
	if userID == "" {
		query = query.Column("user_id").GroupBy("user_id")
	} else {
		query = query.Columns("user_id").
			Where("user_id = ?", userID).
			GroupBy("user_id")
	}
	return query
}

// Conversion helpers to map ClickHouse nullable types to BigQuery-compatible row types

func chUsageSummaryToBqRow(row *ClickhouseUsageSummaryRow) *UsageSummaryRow {
	return &UsageSummaryRow{
		UserID:            row.UserID,
		CreatorID:         toBqNullString(ptrToStr(row.CreatorID)),
		TimeInterval:      row.TimeInterval,
		DeliveryUsageMins: toBqNullFloat64(row.DeliveryUsageMins),
		TotalUsageMins:    toBqNullFloat64(row.TotalUsageMins),
		StorageUsageMins:  toBqNullFloat64(row.StorageUsageMins),
	}
}

func ptrToStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func toBqNullFloat64(p *float64) bigquery.NullFloat64 {
	if p == nil {
		return bigquery.NullFloat64{Valid: false}
	}
	return bigquery.NullFloat64{Float64: *p, Valid: true}
}

func toBqNullString(s string) bigquery.NullString {
	if s == "" {
		return bigquery.NullString{Valid: false}
	}
	return bigquery.NullString{StringVal: s, Valid: true}
}
