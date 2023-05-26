package usage

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/squirrel"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type QueryFilter struct {
	CreatorID string
	UserID    string
}

type QuerySpec struct {
	TimeStep string
	From, To *time.Time
	Filter   QueryFilter
}

var allowedTimeSteps = map[string]bool{
	"hour": true,
	"day":  true,
}

type UsageSummaryRow struct {
	UserID    string `bigquery:"user_id"`
	CreatorID string `bigquery:"creator_id"`

	DeliveryUsageGbs float64 `bigquery:"delivery_usage_gbs"`
	TotalUsageMins   float64 `bigquery:"transcode_total_usage_mins"`
	StorageUsageMins float64 `bigquery:"storage_usage_mins"`
}

type BigQuery interface {
	QueryUsageSummary(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*UsageSummaryRow, error)
	QueryUsageSummaryWithTimestep(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*[]UsageSummaryRow, error)
}

type BigQueryOptions struct {
	BigQueryCredentialsJSON   string
	HourlyUsageTable          string
	MaxBytesBilledPerBigQuery int64
}

const maxBigQueryResultRows = 31

func NewBigQuery(opts BigQueryOptions) (BigQuery, error) {
	bigquery, err := bigquery.NewClient(context.Background(),
		bigquery.DetectProjectID,
		option.WithCredentialsJSON([]byte(opts.BigQueryCredentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &bigqueryHandler{opts, bigquery}, nil
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

func (bq *bigqueryHandler) QueryUsageSummary(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*UsageSummaryRow, error) {
	sql, args, err := buildUsageSummaryQuery(bq.opts.HourlyUsageTable, userID, creatorID, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}

	bqRows, err := doBigQuery[UsageSummaryRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > 1 {
		return nil, fmt.Errorf("internal error, query returned %d rows", len(bqRows))
	}

	if len(bqRows) == 0 {
		return nil, nil
	}
	return &bqRows[0], nil
}

func (bq *bigqueryHandler) QueryUsageSummaryWithTimestep(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*[]UsageSummaryRow, error) {
	sql, args, err := buildUsageSummaryQuery(bq.opts.HourlyUsageTable, userID, creatorID, spec)
	if err != nil {
		return nil, fmt.Errorf("error building usage summary query: %w", err)
	}

	bqRows, err := doBigQuery[UsageSummaryRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	}

	if len(bqRows) == 0 {
		return nil, nil
	}

	return &bqRows, nil
}

func buildUsageSummaryQuery(table string, userID string, creatorID string, spec QuerySpec) (string, []interface{}, error) {
	if userID == "" {
		return "", nil, fmt.Errorf("userID cannot be empty")
	}

	query := squirrel.Select(
		"cast(sum(transcode_total_usage_mins) as FLOAT64) as transcode_total_usage_mins",
		"cast(sum(delivery_usage_gbs) as FLOAT64) as delivery_usage_gbs",
		"cast(avg(storage_usage_mins) as FLOAT64) as storage_usage_mins").
		From(table).
		Limit(maxBigQueryResultRows)

	if creatorId := spec.Filter.CreatorID; creatorId != "" {
		query = query.Where("creator_id_type = ?", "unverified")
		query = query.Where("creator_id = ?", creatorID)
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

	return toTypedValues[RowT](it)
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

		values = append(values, row)
	}
	return values, nil
}
