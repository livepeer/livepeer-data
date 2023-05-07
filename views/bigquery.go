package views

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/squirrel"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const maxBigQueryRows = 10000

type BigQueryOptions struct {
	BigQueryCredentialsJSON   string
	ViewershipEventsTable     string
	ViewershipSummaryTable    string
	MaxBytesBilledPerBigQuery int64
}

// interface from *bigquery.Client to allow mocking
type bigqueryClient interface {
	Query(q string) *bigquery.Query
}

type BigQuery struct {
	opts   BigQueryOptions
	client bigqueryClient
}

func NewBigQuery(opts BigQueryOptions) (*BigQuery, error) {
	bigquery, err := bigquery.NewClient(context.Background(),
		bigquery.DetectProjectID,
		option.WithCredentialsJSON([]byte(opts.BigQueryCredentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &BigQuery{opts, bigquery}, nil
}

// viewership events query

type QueryFilter struct {
	PlaybackID string
	AssetID    string
	StreamID   string
	CreatorID  string
	UserID     string
}

type QuerySpec struct {
	From, To    *time.Time
	TimeStep    string
	Filter      QueryFilter
	BreakdownBy []string
	Detailed    bool
}

// GetSummaryQueryArgs returns the args to be used for a views summary query if
// the provided query can be replaced by a summary query.
// i.e. it is querying for the aggregate metrics of a single playback ID.
func (s QuerySpec) GetSummaryQueryArgs() (playbackID string, ok bool) {
	playbackID = s.Filter.PlaybackID
	pidFilter := QueryFilter{PlaybackID: playbackID, UserID: s.Filter.UserID}

	ok = playbackID != "" && !s.Detailed && s.Filter == pidFilter &&
		s.From == nil && s.To == nil && s.TimeStep == "" &&
		len(s.BreakdownBy) == 0

	if !ok {
		playbackID = ""
	}
	return
}

var viewershipBreakdownFields = map[string]string{
	"device_type":    "device_type",
	"device":         "device",
	"cpu":            "cpu",
	"os":             "os",
	"browser":        "browser",
	"browser_engine": "browser_engine",
	"continent":      "playback_continent_name",
	"country":        "playback_country_name",
	"subdivision":    "playback_subdivisions_name",
	"timezone":       "playback_timezone",
	"viewer_id":      "viewer_id",
}

var allowedTimeSteps = map[string]bool{
	"hour":  true,
	"day":   true,
	"week":  true,
	"month": true,
	"year":  true,
}

func buildViewsEventsQuery(table string, spec QuerySpec) (string, []interface{}, error) {
	query := squirrel.Select(
		"countif(play_intent) as view_count",
		"sum(playtime_ms) / 60000.0 as playtime_mins").
		From(table).
		Where("account_id = ?", spec.Filter.UserID).
		Limit(maxBigQueryRows + 1)
	query = withPlaybackIdFilter(query, spec.Filter.PlaybackID)

	if spec.Detailed {
		query = query.Columns(
			"avg(ttff_ms) as ttff_ms",
			"avg(rebuffer_ratio) as rebuffer_ratio",
			"avg(error_count) as error_rate",
			"avg(if(exit_before_start, 1, 0)) as exits_before_start")
	}

	if spec.Filter.AssetID != "" {
		return "", nil, fmt.Errorf("asset ID filter not supported in the query. translate to playback ID first")
	}
	if creatorId := spec.Filter.CreatorID; creatorId != "" {
		query = query.Where("creator_id_type = ?", "unverified")
		query = query.Where("creator_id = ?", creatorId)
	}

	if timeStep := spec.TimeStep; timeStep != "" {
		if !allowedTimeSteps[timeStep] {
			return "", nil, fmt.Errorf("invalid time step: %s", timeStep)
		}

		query = query.
			Columns(fmt.Sprintf("timestamp_trunc(time, %s) as time_interval", timeStep)).
			GroupBy("time_interval").
			OrderBy("time_interval")
	}

	if from := spec.From; from != nil {
		query = query.Where("time >= timestamp_millis(?)", from.UnixMilli())
	}
	if to := spec.To; to != nil {
		query = query.Where("time < timestamp_millis(?)", to.UnixMilli())
	}

	for _, by := range spec.BreakdownBy {
		field, ok := viewershipBreakdownFields[by]
		if !ok {
			return "", nil, fmt.Errorf("invalid breakdown field: %s", by)
		}
		query = query.Columns(field).GroupBy(field)
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

type ViewershipEventRow struct {
	TimeInterval time.Time `bigquery:"time_interval"`
	PlaybackID   string    `bigquery:"playback_id"`
	DStorageURL  string    `bigquery:"d_storage_url"`

	// breakdown fields

	DeviceType string `bigquery:"device_type"`
	Device     string `bigquery:"device"`
	CPU        string `bigquery:"cpu"`

	OS            string `bigquery:"os"`
	Browser       string `bigquery:"browser"`
	BrowserEngine string `bigquery:"browser_engine"`

	Continent   string `bigquery:"playback_continent_name"`
	Country     string `bigquery:"playback_country_name"`
	Subdivision string `bigquery:"playback_subdivisions_name"`
	TimeZone    string `bigquery:"playback_timezone"`

	// metric data

	ViewCount         int64                `bigquery:"view_count"`
	PlaytimeMins      float64              `bigquery:"playtime_mins"`
	TtffMs            bigquery.NullFloat64 `bigquery:"ttff_ms"`
	RebufferRatio     bigquery.NullFloat64 `bigquery:"rebuffer_ratio"`
	ErrorRate         bigquery.NullFloat64 `bigquery:"error_rate"`
	ExistsBeforeStart bigquery.NullFloat64 `bigquery:"exists_before_start"`
}

func (bq *BigQuery) QueryViewsEvents(ctx context.Context, spec QuerySpec) ([]ViewershipEventRow, error) {
	sql, args, err := buildViewsEventsQuery(bq.opts.ViewershipEventsTable, spec)
	if err != nil {
		return nil, fmt.Errorf("error building viewership events query: %w", err)
	}

	bqRows, err := doBigQuery[ViewershipEventRow](bq, ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryRows)
	}

	return bqRows, nil
}

// viewership summary query

func buildViewsSummaryQuery(table string, playbackID string) (string, []interface{}, error) {
	if playbackID == "" {
		return "", nil, fmt.Errorf("playback ID cannot be empty")
	}

	query := squirrel.Select(
		"cast(sum(view_count) as INT64) as view_count",
		"coalesce(cast(sum(playtime_hrs) as FLOAT64), 0) * 60.0 as playtime_mins").
		From(table).
		Limit(2)
	query = withPlaybackIdFilter(query, playbackID)

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

type ViewSummaryRow struct {
	PlaybackID  string `bigquery:"playback_id"`
	DStorageURL string `bigquery:"d_storage_url"`

	ViewCount    int64   `bigquery:"view_count"`
	PlaytimeMins float64 `bigquery:"playtime_mins"`
}

func (bq *BigQuery) QueryViewsSummary(ctx context.Context, playbackID string) (*ViewSummaryRow, error) {
	sql, args, err := buildViewsSummaryQuery(bq.opts.ViewershipSummaryTable, playbackID)
	if err != nil {
		return nil, fmt.Errorf("error building viewership summary query: %w", err)
	}

	bqRows, err := doBigQuery[ViewSummaryRow](bq, ctx, sql, args)
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

func withPlaybackIdFilter(query squirrel.SelectBuilder, playbackID string) squirrel.SelectBuilder {
	if playbackID == "" {
		query = query.Column("playback_id").GroupBy("playback_id")
	} else if dStorageURL := toDStorageURL(playbackID); dStorageURL != "" {
		query = query.Columns("d_storage_url").
			Where("d_storage_url = ?", dStorageURL).
			GroupBy("d_storage_url")
	} else {
		query = query.Columns("playback_id").
			Where("playback_id = ?", playbackID).
			GroupBy("playback_id")
	}
	return query
}

func doBigQuery[RowT any](bq *BigQuery, ctx context.Context, sql string, args []interface{}) ([]RowT, error) {
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
