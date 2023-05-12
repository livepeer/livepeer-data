package views

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	livepeer "github.com/livepeer/go-api-client"
	promClient "github.com/prometheus/client_golang/api"
)

var ErrAssetNotFound = errors.New("asset not found")

type Metric struct {
	PlaybackID  string `json:"playbackId,omitempty"`
	DStorageURL string `json:"dStorageUrl,omitempty"`
	Timestamp   *int64 `json:"timestamp,omitempty"`

	// breakdown fields

	Device     *string `json:"device,omitempty"`
	DeviceType *string `json:"deviceType,omitempty"`
	CPU        *string `json:"cpu,omitempty"`

	OS            *string `json:"os,omitempty"`
	Browser       *string `json:"browser,omitempty"`
	BrowserEngine *string `json:"browserEngine,omitempty"`

	Continent   *string `json:"continent,omitempty"`
	Country     *string `json:"country,omitempty"`
	Subdivision *string `json:"subdivision,omitempty"`
	TimeZone    *string `json:"timezone,omitempty"`

	// metric data

	ViewCount         int64    `json:"viewCount"`
	PlaytimeMins      float64  `json:"playtimeMins"`
	TtffMs            *float64 `json:"ttffMs,omitempty"`
	RebufferRatio     *float64 `json:"rebufferRatio,omitempty"`
	ErrorRate         *float64 `json:"errorRate,omitempty"`
	ExistsBeforeStart *float64 `json:"existsBeforeStart,omitempty"`
	// Present only on the summary queries. These were imported from the
	// prometheus data we had on the first version of this API and are not
	// shown in the detailed metrics queries (non-/total).
	LegacyViewCount int64 `json:"legacyViewCount,omitempty"`
}

type ClientOptions struct {
	Prometheus promClient.Config
	Livepeer   livepeer.ClientOptions

	BigQueryOptions
}

type Client struct {
	opts     ClientOptions
	lp       *livepeer.Client
	prom     *Prometheus
	bigquery BigQuery
}

func NewClient(opts ClientOptions) (*Client, error) {
	lp := livepeer.NewAPIClient(opts.Livepeer)

	prom, err := NewPrometheus(opts.Prometheus)
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus client: %w", err)
	}

	bigquery, err := NewBigQuery(opts.BigQueryOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &Client{opts, lp, prom, bigquery}, nil
}

func (c *Client) Deprecated_GetTotalViews(ctx context.Context, id string) ([]TotalViews, error) {
	asset, err := c.lp.GetAsset(id, false)
	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, ErrAssetNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset: %w", err)
	}

	startViews, err := c.prom.QueryStartViews(ctx, asset)
	if err != nil {
		return nil, fmt.Errorf("error querying start views: %w", err)
	}

	return []TotalViews{{
		ID:         asset.PlaybackID,
		StartViews: startViews,
	}}, nil
}

func (c *Client) QuerySummary(ctx context.Context, playbackID string) (*Metric, error) {
	summary, err := c.bigquery.QueryViewsSummary(ctx, playbackID)
	if err != nil {
		return nil, err
	}

	metrics := viewershipSummaryToMetric(playbackID, summary)
	return metrics, nil
}

func viewershipSummaryToMetric(playbackID string, summary *ViewSummaryRow) *Metric {
	if summary == nil {
		return nil
	}

	return &Metric{
		PlaybackID:      summary.PlaybackID,
		DStorageURL:     summary.DStorageURL,
		ViewCount:       summary.ViewCount,
		LegacyViewCount: summary.LegacyViewCount,
		PlaytimeMins:    summary.PlaytimeMins,
	}
}

func (c *Client) QueryEvents(ctx context.Context, spec QuerySpec, assetID, streamID string) ([]Metric, error) {
	var err error
	if assetID != "" {
		var asset *livepeer.Asset

		asset, err = c.lp.GetAsset(assetID, false)
		if asset != nil {
			spec.Filter.PlaybackID = asset.PlaybackID
		}
	} else if streamID != "" {
		var stream *livepeer.Stream

		stream, err = c.lp.GetStream(streamID, false)
		if stream != nil {
			spec.Filter.PlaybackID = stream.PlaybackID
		}
	}

	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, ErrAssetNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset or stream: %w", err)
	}

	rows, err := c.bigquery.QueryViewsEvents(ctx, spec)
	if err != nil {
		return nil, err
	}

	metrics := viewershipEventsToMetrics(rows)
	return metrics, nil
}

func viewershipEventsToMetrics(rows []ViewershipEventRow) []Metric {
	metrics := make([]Metric, len(rows))
	for i, row := range rows {
		m := Metric{
			PlaybackID:        row.PlaybackID,
			DStorageURL:       row.DStorageURL,
			Device:            toStringPtr(row.Device),
			OS:                toStringPtr(row.OS),
			Browser:           toStringPtr(row.Browser),
			Continent:         toStringPtr(row.Continent),
			Country:           toStringPtr(row.Country),
			Subdivision:       toStringPtr(row.Subdivision),
			TimeZone:          toStringPtr(row.TimeZone),
			ViewCount:         row.ViewCount,
			PlaytimeMins:      row.PlaytimeMins,
			TtffMs:            toFloat64Ptr(row.TtffMs),
			RebufferRatio:     toFloat64Ptr(row.RebufferRatio),
			ErrorRate:         toFloat64Ptr(row.ErrorRate),
			ExistsBeforeStart: toFloat64Ptr(row.ExistsBeforeStart),
		}

		if !row.TimeInterval.IsZero() {
			timestamp := row.TimeInterval.UnixMilli()
			m.Timestamp = &timestamp
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

func toStringPtr(bqFloat bigquery.NullString) *string {
	if bqFloat.Valid {
		f := bqFloat.StringVal
		return &f
	}
	return nil
}
