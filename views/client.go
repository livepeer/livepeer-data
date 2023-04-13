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
)

var ErrAssetNotFound = errors.New("asset not found")

type TotalViews struct {
	ID         string `json:"id"`
	StartViews int64  `json:"startViews"`
	ViewCount  int64  `json:"viewCount"`
	PlaytimeMs int64  `json:"playtimeMs"`
}

type ClientOptions struct {
	Prometheus              promClient.Config
	Livepeer                livepeer.ClientOptions
	BigQueryCredentialsJSON string
}

type Client struct {
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

	return &Client{prom, lp, bigquery}, nil
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

	viewCount, playtimeMs, err := c.doStartViewsBigQuery(ctx, asset.PlaybackID)
	if err != nil {
		return nil, err
	}

	return []TotalViews{{
		ID:         asset.PlaybackID,
		StartViews: startViews,
		ViewCount:  viewCount,
		PlaytimeMs: playtimeMs,
	}}, nil
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

func (c *Client) doStartViewsBigQuery(ctx context.Context, playbackID string) (views, playtimeMs int64, err error) {
	type ResultRow struct {
		// TimeInterval time.Time `bigquery:"time_interval"`
		PlaybackID string `bigquery:"playback_id"`
		ViewCount  int64  `bigquery:"view_count"`
		PlaytimeMs int64  `bigquery:"playtime_ms"`
	}
	query := squirrel.Select(
		// "timestamp_trunc(time_interval_ts, hour) as time_interval",
		"playback_id",
		"sum(view_count) as view_count",
		"sum(playtime_ms) as playtime_ms",
	).
		From("analytics.ldsm_state").
		// Where("time_interval_ts > current_timestamp() - interval 7 day").
		Where("playback_id = ?", playbackID).
		GroupBy(
			//"time_interval",
			"playback_id")

	bqRows, err := doBigQuery[ResultRow](c, ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("bigquery error: %w", err)
	} else if len(bqRows) > 1 {
		return 0, 0, fmt.Errorf("unexpected result count: %d", len(bqRows))
	}

	if len(bqRows) == 0 {
		return 0, 0, nil
	}
	return bqRows[0].ViewCount, bqRows[0].PlaytimeMs, nil
}

func doBigQuery[RowT any](c *Client, ctx context.Context, squerry squirrel.SelectBuilder) ([]RowT, error) {
	sql, args, err := squerry.ToSql()
	if err != nil {
		return nil, fmt.Errorf("error building query: %w", err)
	}

	query := c.bigquery.Query(sql)
	query.Parameters = toBigQueryParameters(args)

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
