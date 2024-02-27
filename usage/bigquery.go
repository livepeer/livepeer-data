package usage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/squirrel"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type QueryFilter struct {
	CreatorID string
	UserID    string
}

type QuerySpec struct {
	TimeStep    string
	From, To    *time.Time
	Filter      QueryFilter
	BreakdownBy []string
}

var usageBreakdownFields = map[string]string{
	"creatorId": "creator_id",
}

type FromToQuerySpec struct {
	From, To *time.Time
}

var allowedTimeSteps = map[string]bool{
	"hour": true,
	"day":  true,
}

type UsageSummaryRow struct {
	UserID       string              `bigquery:"user_id"`
	CreatorID    bigquery.NullString `bigquery:"creator_id"`
	TimeInterval time.Time           `bigquery:"time_interval"`

	DeliveryUsageMins bigquery.NullFloat64 `bigquery:"delivery_usage_mins"`
	TotalUsageMins    bigquery.NullFloat64 `bigquery:"transcode_total_usage_mins"`
	StorageUsageMins  bigquery.NullFloat64 `bigquery:"storage_usage_mins"`
}

type ActiveUsersSummaryRow struct {
	UserID string    `bigquery:"user_id" json:"userId"`
	Email  string    `bigquery:"email" json:"email"`
	From   time.Time `bigquery:"interval_start_date" json:"from"`
	To     time.Time `bigquery:"interval_end_date" json:"to"`

	DeliveryUsageMins float64 `bigquery:"delivery_usage_mins" json:"deliveryUsageMins"`
	TotalUsageMins    float64 `bigquery:"transcode_total_usage_mins" json:"totalUsageMins"`
	StorageUsageMins  float64 `bigquery:"storage_usage_mins" json:"storageUsageMins"`
}

type TotalUsageSummaryRow struct {
	DateTs                time.Time `bigquery:"date_ts" json:"dateTs"`
	DateS                 int64     `bigquery:"date_s" json:"dateS"`
	WeekTs                time.Time `bigquery:"week_ts" json:"weekTs"`
	WeekS                 int64     `bigquery:"week_s" json:"weekS"`
	VolumeEth             float64   `bigquery:"volume_eth" json:"volumeEth"`
	VolumeUsd             float64   `bigquery:"volume_usd" json:"volumeUsd"`
	FeeDerivedMinutes     float64   `bigquery:"fee_derived_minutes" json:"feeDerivedMinutes"`
	ParticipationRate     float64   `bigquery:"participation_rate" json:"participationRate"`
	Inflation             float64   `bigquery:"inflation" json:"inflation"`
	ActiveTranscoderCount int64     `bigquery:"active_transcoder_count" json:"activeTranscoderCount"`
	DelegatorsCount       int64     `bigquery:"delegators_count" json:"delegatorsCount"`
	AveragePricePerPixel  float64   `bigquery:"average_price_per_pixel" json:"averagePricePerPixel"`
	AveragePixelPerMinute float64   `bigquery:"average_pixel_per_minute" json:"averagePixelPerMinute"`
}

type BigQuery interface {
	QueryUsageSummary(ctx context.Context, userID string, spec QuerySpec) ([]UsageSummaryRow, error)
	QueryUsageSummaryWithTimestep(ctx context.Context, userID string, spec QuerySpec) ([]UsageSummaryRow, error)
	QueryTotalUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]TotalUsageSummaryRow, error)
	QueryActiveUsersUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]ActiveUsersSummaryRow, error)
}

type BigQueryOptions struct {
	BigQueryCredentialsJSON   string
	HourlyUsageTable          string
	DailyUsageTable           string
	UsersTable                string
	MaxBytesBilledPerBigQuery int64
}

const maxBigQueryResultRows = 10000

func NewBigQuery(opts BigQueryOptions) (BigQuery, error) {
	bigquery, err := bigquery.NewClient(context.Background(),
		bigquery.DetectProjectID,
		option.WithCredentialsJSON([]byte(opts.BigQueryCredentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &bigqueryHandler{opts, bigquery}, nil
}

func parseInputTimestamp(str string) (*time.Time, error) {
	if str == "" {
		return nil, nil
	}
	t, rfcErr := time.Parse(time.RFC3339Nano, str)
	if rfcErr == nil {
		return &t, nil
	}

	ts, unixErr := strconv.ParseInt(str, 10, 64)
	if unixErr != nil {
		return nil, fmt.Errorf("bad time %q. must be in RFC3339 or Unix Timestamp (millisecond) formats. rfcErr: %s; unixErr: %s", str, rfcErr, unixErr)
	}
	t = time.UnixMilli(ts)
	return &t, nil
}

// interface from *bigquery.Client to allow mocking
type bigqueryClient interface {
	Query(q string) *bigquery.Query
}

type bigqueryHandler struct {
	opts   BigQueryOptions
	client bigqueryClient
}

// usage summary query

func (bq *bigqueryHandler) QueryUsageSummary(ctx context.Context, userID string, spec QuerySpec) ([]UsageSummaryRow, error) {
	glog.V(10).Infof("QueryUsageSummary: userID=%s, creatorID=%s, spec=%+v", userID, spec)
	sql, args, err := buildUsageSummaryQuery(bq.opts.HourlyUsageTable, userID, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}
	glog.V(10).Infof("QueryUsageSummary: sql=%s, args=%v", sql, args)
	bqRows, err := doBigQuery[UsageSummaryRow](bq, ctx, sql, args)
	// glog.V(10).Infof("QueryUsageSummary: bqRows=%s", bqRows)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryResultRows)
	}

	if len(bqRows) == 0 {
		return nil, nil
	}

	return bqRows, nil
}

func (bq *bigqueryHandler) QueryUsageSummaryWithTimestep(ctx context.Context, userID string, spec QuerySpec) ([]UsageSummaryRow, error) {
	glog.V(10).Infof("QueryUsageSummaryWithTimestep: userID=%s, spec=%+v", userID, spec)

	sql, args, err := buildUsageSummaryQuery(bq.opts.HourlyUsageTable, userID, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}
	glog.V(10).Infof("QueryUsageSummaryWithTimestep: sql=%s, args=%v", sql, args)

	bqRows, err := doBigQuery[UsageSummaryRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryResultRows)
	}

	if len(bqRows) == 0 {
		return nil, nil
	}

	return bqRows, nil
}

func (bq *bigqueryHandler) QueryTotalUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]TotalUsageSummaryRow, error) {
	sql, args, err := buildTotalUsageSummaryQuery(bq.opts.DailyUsageTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}

	bqRows, err := doBigQuery[TotalUsageSummaryRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryResultRows)
	}

	if len(bqRows) == 0 {
		return nil, nil
	}

	return bqRows, nil
}

func (bq *bigqueryHandler) QueryActiveUsersUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]ActiveUsersSummaryRow, error) {
	sql, args, err := buildActiveUsersUsageSummaryQuery(bq.opts.HourlyUsageTable, bq.opts.UsersTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building active users summary query: %w", err)
	}

	bqRows, err := doBigQuery[ActiveUsersSummaryRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryResultRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryResultRows)
	}

	if len(bqRows) == 0 {
		return nil, nil
	}

	return bqRows, nil
}

func buildUsageSummaryQuery(table string, userID string, spec QuerySpec) (string, []interface{}, error) {
	if userID == "" {
		return "", nil, fmt.Errorf("userID cannot be empty")
	}

	query := squirrel.Select(
		"cast(sum(transcode_total_usage_mins) as FLOAT64) as transcode_total_usage_mins",
		"cast(sum(delivery_usage_mins) as FLOAT64) as delivery_usage_mins",
		"cast((sum(storage_usage_mins) / count(distinct usage_hour_ts)) as FLOAT64) as storage_usage_mins").
		From(table).
		Limit(maxBigQueryResultRows + 1)

	if creatorId := spec.Filter.CreatorID; creatorId != "" {
		query = query.Columns("creator_id").Where("creator_id = ?", creatorId).GroupBy("creator_id")
	}

	if from := spec.From; from != nil {
		query = query.Where("usage_hour_ts >= timestamp_millis(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("usage_hour_ts < timestamp_millis(?)", to.UnixMilli())
	}

	if timeStep := spec.TimeStep; timeStep != "" {
		if !allowedTimeSteps[timeStep] {
			return "", nil, fmt.Errorf("invalid time step: %s", timeStep)
		}

		query = query.
			Columns(fmt.Sprintf("timestamp_trunc(usage_hour_ts, %s) as time_interval", timeStep)).
			GroupBy("time_interval").
			OrderBy("time_interval")
	}

	query = withUserIdFilter(query, userID)

	for _, by := range spec.BreakdownBy {
		field, ok := usageBreakdownFields[by]
		if !ok {
			return "", nil, fmt.Errorf("invalid breakdown field: %s", by)
		}
		// skip breakdowns that are already in the query
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

func buildTotalUsageSummaryQuery(table string, spec FromToQuerySpec) (string, []interface{}, error) {

	query := squirrel.Select(
		"date_ts,date_s,week_ts,week_s,volume_eth,volume_usd, fee_derived_minutes,participation_rate,inflation,active_transcoder_count,delegators_count,average_price_per_pixel,average_pixel_per_minute").
		From(table).
		Limit(maxBigQueryResultRows + 1).
		OrderBy("date_ts DESC")

	if from := spec.From; from != nil {
		query = query.Where("date_ts >= timestamp_millis(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("date_ts < timestamp_millis(?)", to.UnixMilli())
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

func buildActiveUsersUsageSummaryQuery(billingTable, usersTable string, spec FromToQuerySpec) (string, []interface{}, error) {

	// Create the base select statement using the provided billingTable and usersTable
	query := squirrel.
		Select(
			"b.user_id",
			"u.email",
			"min(usage_hour_ts) as interval_start_date",
			"max(usage_hour_ts) as interval_end_date",
			"cast(sum(transcode_total_usage_mins) as FLOAT64) as transcode_total_usage_mins",
			"cast(sum(delivery_usage_mins) as FLOAT64) as delivery_usage_mins",
			"cast((sum(storage_usage_mins) / count(distinct usage_hour_ts)) as FLOAT64) as storage_usage_mins",
		).
		From(fmt.Sprintf("`%s` as b", billingTable)).
		Join(fmt.Sprintf("%s as u on b.user_id = u.user_id", usersTable)).
		Where("not internal").
		GroupBy("b.user_id", "u.email").
		Having("transcode_total_usage_mins > 0 or delivery_usage_mins > 0 or storage_usage_mins > 0")

	// Apply additional conditions based on the spec provided
	if from := spec.From; from != nil {
		query = query.Where("usage_hour_ts >= timestamp_millis(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("usage_hour_ts < timestamp_millis(?)", to.UnixMilli())
	}

	// Convert to SQL
	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

// query helpers

func withUserIdFilter(query squirrel.SelectBuilder, userID string) squirrel.SelectBuilder {
	if userID == "" {
		query = query.Column("user_id").GroupBy("user_id")
	} else {
		query = query.Columns("user_id").
			Where("user_id = ?", userID).
			GroupBy("user_id")
	}
	return query
}

func doBigQuery[RowT any](bq *bigqueryHandler, ctx context.Context, sql string, args []interface{}) ([]RowT, error) {
	query := bq.client.Query(sql)
	query.Parameters = toBigQueryParameters(args)
	query.MaxBytesBilled = bq.opts.MaxBytesBilledPerBigQuery

	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("error running query: %w", err)
	}

	typed, err := toTypedValues[RowT](it)
	// glog.V(10).Infof("typed: %s", typed)

	return typed, err
}

func toBigQueryParameters(args []interface{}) []bigquery.QueryParameter {
	params := make([]bigquery.QueryParameter, len(args))
	for i, arg := range args {
		params[i] = bigquery.QueryParameter{Value: arg}
	}
	return params
}

func toTypedValues[RowT any](it *bigquery.RowIterator) ([]RowT, error) {
	var values []RowT
	for {
		var row RowT
		err := it.Next(&row)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("error reading query result: %w", err)
		}
		glog.V(10).Infof("values: %s", row)

		values = append(values, row)
	}
	glog.V(10).Infof("values: %s %d", values[0], len(values))
	return values, nil
}
