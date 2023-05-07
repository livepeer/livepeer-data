package views

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/squirrel"
	"github.com/golang/glog"
	livepeer "github.com/livepeer/go-api-client"
	promClient "github.com/prometheus/client_golang/api"
	prometheus "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var ErrAssetNotFound = errors.New("asset not found")

const maxBigQueryRows = 10000

type TotalViews struct {
	ID         string `json:"id"`
	StartViews int64  `json:"startViews"`
}

type Metric struct {
	PlaybackID  string     `json:"playbackId,omitempty"`
	DStorageURL string     `json:"dStorageUrl,omitempty"`
	Time        *time.Time `json:"time,omitempty"`

	// breakdown fields

	Device     string `json:"device,omitempty"`
	DeviceType string `json:"deviceType,omitempty"`
	CPU        string `json:"cpu,omitempty"`

	OS            string `json:"os,omitempty"`
	Browser       string `json:"browser,omitempty"`
	BrowserEngine string `json:"browserEngine,omitempty"`

	Continent   string `json:"continent,omitempty"`
	Country     string `json:"country,omitempty"`
	Subdivision string `json:"subdivision,omitempty"`
	TimeZone    string `json:"timezone,omitempty"`

	// metric data

	ViewCount         int64    `json:"viewCount"`
	PlaytimeMins      float64  `json:"playtimeMins"`
	TtffMs            *float64 `json:"ttffMs,omitempty"`
	RebufferRatio     *float64 `json:"rebufferRatio,omitempty"`
	ErrorRate         *float64 `json:"errorRate,omitempty"`
	ExistsBeforeStart *float64 `json:"existsBeforeStart,omitempty"`
}

type ClientOptions struct {
	Prometheus promClient.Config
	Livepeer   livepeer.ClientOptions

	BigQueryCredentialsJSON   string
	ViewershipEventsTable     string
	ViewershipSummaryTable    string
	MaxBytesBilledPerBigQuery int64
}

type Client struct {
	opts     ClientOptions
	prom     prometheus.API
	lp       *livepeer.Client
	bigquery *bigquery.Client
}

func NewClient(opts ClientOptions) (*Client, error) {
	client, err := promClient.NewClient(opts.Prometheus)
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus client: %w", err)
	}
	prom := prometheus.NewAPI(client)
	lp := livepeer.NewAPIClient(opts.Livepeer)

	bigquery, err := bigquery.NewClient(context.Background(),
		bigquery.DetectProjectID,
		option.WithCredentialsJSON([]byte(opts.BigQueryCredentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &Client{opts, prom, lp, bigquery}, nil
}

func (c *Client) GetTotalViews(ctx context.Context, id string) ([]TotalViews, error) {
	asset, err := c.lp.GetAsset(id, false)
	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, ErrAssetNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset: %w", err)
	}

	startViews, err := c.doQueryStartViews(ctx, asset)
	if err != nil {
		return nil, fmt.Errorf("error querying start views: %w", err)
	}

	return []TotalViews{{
		ID:         asset.PlaybackID,
		StartViews: startViews,
	}}, nil
}

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

func (s QuerySpec) IsSummaryQuery() bool {
	playbackID := s.Filter.PlaybackID
	pidFilter := QueryFilter{PlaybackID: playbackID, UserID: s.Filter.UserID}

	return playbackID != "" && !s.Detailed && s.Filter == pidFilter &&
		s.From == nil && s.To == nil && s.TimeStep == "" &&
		len(s.BreakdownBy) == 0
}

func (c *Client) Query(ctx context.Context, spec QuerySpec) ([]Metric, error) {
	var err error
	if spec.Filter.PlaybackID != "" {
		_, err = c.lp.GetAssetByPlaybackID(spec.Filter.PlaybackID, false)

		// TODO: remove this hack once we have staging data to test
		if errors.Is(err, livepeer.ErrNotExists) {
			err = nil
		}
	} else if spec.Filter.AssetID != "" {
		var asset *livepeer.Asset

		asset, err = c.lp.GetAsset(spec.Filter.AssetID, false)
		if asset != nil {
			spec.Filter.AssetID, spec.Filter.PlaybackID = "", asset.PlaybackID
		}
	} else if spec.Filter.StreamID != "" {
		var stream *livepeer.Stream

		stream, err = c.lp.GetStream(spec.Filter.StreamID, false)
		if stream != nil {
			spec.Filter.AssetID, spec.Filter.PlaybackID = "", stream.PlaybackID
		}
	}

	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, ErrAssetNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset or stream: %w", err)
	}

	var metrics []Metric
	if spec.IsSummaryQuery() {
		summary, err := c.doViewSummaryBigQuery(ctx, spec.Filter.PlaybackID)
		if err != nil {
			return nil, err
		}

		metrics = viewershipSummaryToMetric(spec.Filter, summary)
	} else {
		rows, err := c.doViewsEventsBigQuery(ctx, spec)
		if err != nil {
			return nil, err
		}

		metrics = viewershipEventsToMetrics(rows)
	}

	return metrics, nil
}

func viewershipEventsToMetrics(rows []ViewershipEventRow) []Metric {
	metrics := make([]Metric, len(rows))
	for i, row := range rows {
		m := Metric{
			PlaybackID:        row.PlaybackID,
			DStorageURL:       row.DStorageURL,
			Device:            row.Device,
			OS:                row.OS,
			Browser:           row.Browser,
			Continent:         row.Continent,
			Country:           row.Country,
			Subdivision:       row.Subdivision,
			TimeZone:          row.TimeZone,
			ViewCount:         row.ViewCount,
			PlaytimeMins:      row.PlaytimeMins,
			TtffMs:            toFloat64Ptr(row.TtffMs),
			RebufferRatio:     toFloat64Ptr(row.RebufferRatio),
			ErrorRate:         toFloat64Ptr(row.ErrorRate),
			ExistsBeforeStart: toFloat64Ptr(row.ExistsBeforeStart),
		}

		if !row.TimeInterval.IsZero() {
			m.Time = &row.TimeInterval
		}

		metrics[i] = m
	}
	return metrics
}

func toFloat64Ptr(bqFloat bigquery.NullFloat64) *float64 {
	if bqFloat.Valid {
		f := bqFloat.Float64
		return &f
	}
	return nil
}

func viewershipSummaryToMetric(filter QueryFilter, summary *ViewSummaryRow) []Metric {
	if summary == nil {
		if dStorageURL := toDStorageURL(filter.PlaybackID); dStorageURL != "" {
			return []Metric{{DStorageURL: dStorageURL}}
		}
		return []Metric{{PlaybackID: filter.PlaybackID}}
	}

	return []Metric{{
		PlaybackID:   summary.PlaybackID,
		DStorageURL:  summary.DStorageURL,
		ViewCount:    summary.ViewCount,
		PlaytimeMins: summary.PlaytimeMins,
	}}
}

func (c *Client) doQueryStartViews(ctx context.Context, asset *livepeer.Asset) (int64, error) {
	query := startViewsQuery(asset.PlaybackID, asset.PlaybackRecordingID)
	value, warn, err := c.prom.Query(ctx, query, time.Time{})
	if len(warn) > 0 {
		glog.Warningf("Prometheus query warnings: %q", warn)
	}
	if err != nil {
		return -1, fmt.Errorf("query error: %w", err)
	}
	if value.Type() != model.ValVector {
		return -1, fmt.Errorf("unexpected value type: %s", value.Type())
	}
	vec := value.(model.Vector)
	if len(vec) > 1 {
		return -1, fmt.Errorf("unexpected result count: %d", len(vec))
	} else if len(vec) == 0 {
		return 0, nil
	}
	return int64(vec[0].Value), nil
}

func startViewsQuery(playbackID, playbackRecordingID string) string {
	queryID := playbackID
	if playbackRecordingID != "" {
		queryID = fmt.Sprintf("(%s|%s)", playbackID, playbackRecordingID)
	}
	return fmt.Sprintf(
		`sum(increase(mist_playux_count{strm=~"video(rec)?\\+%s"} [1y]))`,
		queryID,
	)
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

func (c *Client) doViewsEventsBigQuery(ctx context.Context, spec QuerySpec) ([]ViewershipEventRow, error) {
	query := squirrel.Select(
		"countif(play_intent) as view_count",
		"sum(playtime_ms) / 60000.0 as playtime_mins").
		From(c.opts.ViewershipEventsTable).
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
		return nil, fmt.Errorf("asset ID filter not supported in the query. translate to playback ID first")
	}
	if creatorId := spec.Filter.CreatorID; creatorId != "" {
		query = query.Where("creator_id_type = ?", "unverified")
		query = query.Where("creator_id = ?", creatorId)
	}

	if timeStep := spec.TimeStep; timeStep != "" {
		if !allowedTimeSteps[timeStep] {
			return nil, fmt.Errorf("invalid time step: %s", timeStep)
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
			return nil, fmt.Errorf("invalid breakdown field: %s", by)
		}
		query = query.Columns(field).GroupBy(field)
	}

	bqRows, err := doBigQuery[ViewershipEventRow](c, ctx, query)
	if err != nil {
		return nil, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > maxBigQueryRows {
		return nil, fmt.Errorf("query must return less than %d datapoints. consider decreasing your timeframe or increasing the time step", maxBigQueryRows)
	}

	return bqRows, nil
}

type ViewSummaryRow struct {
	PlaybackID  string `bigquery:"playback_id"`
	DStorageURL string `bigquery:"d_storage_url"`

	ViewCount    int64   `bigquery:"view_count"`
	PlaytimeMins float64 `bigquery:"playtime_mins"`
}

func (c *Client) doViewSummaryBigQuery(ctx context.Context, playbackID string) (*ViewSummaryRow, error) {
	if playbackID == "" {
		return nil, fmt.Errorf("playback ID cannot be empty")
	}

	query := squirrel.Select(
		"cast(sum(view_count) as INT64) as view_count",
		"coalesce(cast(sum(playtime_hrs) as FLOAT64), 0) * 60.0 as playtime_mins").
		From(c.opts.ViewershipSummaryTable).
		Limit(2)
	query = withPlaybackIdFilter(query, playbackID)

	bqRows, err := doBigQuery[ViewSummaryRow](c, ctx, query)
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

func doBigQuery[RowT any](c *Client, ctx context.Context, squerry squirrel.SelectBuilder) ([]RowT, error) {
	sql, args, err := squerry.ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building query: %w", err)
	}

	query := c.bigquery.Query(sql)
	query.Parameters = toBigQueryParameters(args)
	query.MaxBytesBilled = c.opts.MaxBytesBilledPerBigQuery

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
