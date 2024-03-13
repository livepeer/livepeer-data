package views

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	livepeer "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	promClient "github.com/prometheus/client_golang/api"
)

var ErrAssetNotFound = errors.New("asset not found")

type Metric struct {
	Timestamp *int64 `json:"timestamp,omitempty"`

	// breakdown fields

	CreatorID   data.Nullable[string] `json:"creatorId,omitempty"`
	ViewerID    data.Nullable[string] `json:"viewerId,omitempty"`
	PlaybackID  data.Nullable[string] `json:"playbackId,omitempty"`
	DStorageURL data.Nullable[string] `json:"dStorageUrl,omitempty"`

	Device     data.Nullable[string] `json:"device,omitempty"`
	DeviceType data.Nullable[string] `json:"deviceType,omitempty"`
	CPU        data.Nullable[string] `json:"cpu,omitempty"`

	OS            data.Nullable[string] `json:"os,omitempty"`
	Browser       data.Nullable[string] `json:"browser,omitempty"`
	BrowserEngine data.Nullable[string] `json:"browserEngine,omitempty"`

	Continent   data.Nullable[string] `json:"continent,omitempty"`
	Country     data.Nullable[string] `json:"country,omitempty"`
	Subdivision data.Nullable[string] `json:"subdivision,omitempty"`
	TimeZone    data.Nullable[string] `json:"timezone,omitempty"`
	GeoHash     data.Nullable[string] `json:"geohash,omitempty"`

	// metric data

	ViewCount        int64                  `json:"viewCount"`
	PlaytimeMins     data.Nullable[float64] `json:"playtimeMins,omitempty"`
	TtffMs           data.Nullable[float64] `json:"ttffMs,omitempty"`
	RebufferRatio    data.Nullable[float64] `json:"rebufferRatio,omitempty"`
	ErrorRate        data.Nullable[float64] `json:"errorRate,omitempty"`
	ExitsBeforeStart data.Nullable[float64] `json:"exitsBeforeStart,omitempty"`
	// Present only on the summary queries. These were imported from the
	// prometheus data we had on the first version of this API and are not
	// shown in the detailed metrics queries (non-/total).
	LegacyViewCount data.Nullable[int64] `json:"legacyViewCount,omitempty"`
}

type ClientOptions struct {
	Prometheus promClient.Config
	Livepeer   livepeer.ClientOptions

	BigQueryOptions
	ClickhouseOptions
}

type Client struct {
	opts       ClientOptions
	lp         *livepeer.Client
	prom       *Prometheus
	bigquery   BigQuery
	clickhouse Clickhouse
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

	clickhouse, err := NewClickhouseConn(opts.ClickhouseOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating clickhouse client: %w", err)
	}

	return &Client{opts, lp, prom, bigquery, clickhouse}, nil
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

	// We never want to return `null` for the legacy view count, so we don't use
	// the regular nullable creation.
	legacyViewCount := int64(0)
	if summary.LegacyViewCount.Valid {
		legacyViewCount = summary.LegacyViewCount.Int64
	}

	return &Metric{
		PlaybackID:      bqToStringPtr(summary.PlaybackID, summary.PlaybackID.Valid),
		DStorageURL:     bqToStringPtr(summary.DStorageURL, summary.DStorageURL.Valid),
		ViewCount:       summary.ViewCount,
		LegacyViewCount: data.WrapNullable(legacyViewCount),
		PlaytimeMins:    data.WrapNullable(summary.PlaytimeMins),
	}
}

func (c *Client) QueryEvents(ctx context.Context, spec QuerySpec) ([]Metric, error) {
	rows, err := c.bigquery.QueryViewsEvents(ctx, spec)
	if err != nil {
		return nil, err
	}

	metrics := viewershipEventsToMetrics(rows, spec)
	return metrics, nil
}

func (c *Client) QueryRealtimeEvents(ctx context.Context, spec QuerySpec) ([]Metric, error) {
	rows, err := c.clickhouse.QueryRealtimeViewsEvents(ctx, spec)
	if err != nil {
		return nil, err
	}
	metrics := realtimeViewershipEventsToMetrics(rows, spec)
	return metrics, nil
}

func (c *Client) Validate(spec QuerySpec, assetID, streamID string) error {
	var err error
	if assetID != "" {
		var asset *livepeer.Asset

		asset, err = c.lp.GetAsset(assetID, false)
		if asset != nil {
			spec.Filter.PlaybackID = asset.PlaybackID
			if spec.Filter.UserID != asset.UserID {
				return fmt.Errorf("error getting asset: verify that asset exists and you are using proper credentials")
			}
		}
	} else if streamID != "" {
		var stream *livepeer.Stream

		stream, err = c.lp.GetStream(streamID, false)
		if stream != nil {
			spec.Filter.PlaybackID = stream.PlaybackID
			if spec.Filter.UserID != stream.UserID {
				return fmt.Errorf("error getting stream: verify that stream exists and you are using proper credentials")
			}
		}
	}

	if errors.Is(err, livepeer.ErrNotExists) {
		return ErrAssetNotFound
	} else if err != nil {
		return fmt.Errorf("error getting asset or stream: %w", err)
	}
	return nil
}

func viewershipEventsToMetrics(rows []ViewershipEventRow, spec QuerySpec) []Metric {
	metrics := make([]Metric, len(rows))
	for i, row := range rows {
		m := Metric{
			CreatorID:        bqToStringPtr(row.CreatorID, spec.hasBreakdownBy("creatorId")),
			ViewerID:         bqToStringPtr(row.ViewerID, spec.hasBreakdownBy("viewerId")),
			PlaybackID:       bqToStringPtr(row.PlaybackID, spec.hasBreakdownBy("playbackId")),
			DStorageURL:      bqToStringPtr(row.DStorageURL, spec.hasBreakdownBy("dStorageUrl")),
			Device:           bqToStringPtr(row.Device, spec.hasBreakdownBy("device")),
			OS:               bqToStringPtr(row.OS, spec.hasBreakdownBy("os")),
			Browser:          bqToStringPtr(row.Browser, spec.hasBreakdownBy("browser")),
			Continent:        bqToStringPtr(row.Continent, spec.hasBreakdownBy("continent")),
			Country:          bqToStringPtr(row.Country, spec.hasBreakdownBy("country")),
			Subdivision:      bqToStringPtr(row.Subdivision, spec.hasBreakdownBy("subdivision")),
			TimeZone:         bqToStringPtr(row.TimeZone, spec.hasBreakdownBy("timezone")),
			GeoHash:          bqToStringPtr(row.GeoHash, spec.hasBreakdownBy("geohash")),
			ViewCount:        row.ViewCount,
			PlaytimeMins:     data.WrapNullable(row.PlaytimeMins),
			TtffMs:           bqToFloat64Ptr(row.TtffMs, spec.Detailed),
			RebufferRatio:    bqToFloat64Ptr(row.RebufferRatio, spec.Detailed),
			ErrorRate:        bqToFloat64Ptr(row.ErrorRate, spec.Detailed),
			ExitsBeforeStart: bqToFloat64Ptr(row.ExitsBeforeStart, spec.Detailed),
		}

		if !row.TimeInterval.IsZero() {
			timestamp := row.TimeInterval.UnixMilli()
			m.Timestamp = &timestamp
		}

		metrics[i] = m
	}
	return metrics
}

func realtimeViewershipEventsToMetrics(rows []RealtimeViewershipRow, spec QuerySpec) []Metric {
	metrics := make([]Metric, len(rows))
	isTimeRange := spec.From != nil || spec.To != nil
	for i, row := range rows {
		m := Metric{
			ViewCount:     int64(row.ViewCount),
			RebufferRatio: toFloat64Ptr(row.BufferRatio, isTimeRange),
			ErrorRate:     toFloat64Ptr(row.ErrorRate, isTimeRange),
			PlaybackID:    toStringPtr(row.PlaybackID, spec.hasBreakdownBy("playbackId")),
			DeviceType:    toStringPtr(row.DeviceType, spec.hasBreakdownBy("deviceType")),
			Browser:       toStringPtr(row.Browser, spec.hasBreakdownBy("browser")),
			Country:       toStringPtr(row.CountryName, spec.hasBreakdownBy("country")),
		}

		if !row.Timestamp.IsZero() {
			timestamp := row.Timestamp.UnixMilli()
			m.Timestamp = &timestamp
		}

		metrics[i] = m
	}
	return metrics
}

func bqToFloat64Ptr(bqFloat bigquery.NullFloat64, asked bool) data.Nullable[float64] {
	return data.ToNullable(bqFloat.Float64, bqFloat.Valid, asked)
}

func bqToStringPtr(bqStr bigquery.NullString, asked bool) data.Nullable[string] {
	return data.ToNullable(bqStr.StringVal, bqStr.Valid, asked)
}

func toFloat64Ptr(f float64, asked bool) data.Nullable[float64] {
	return data.ToNullable(f, true, asked)
}

func toStringPtr(s string, asked bool) data.Nullable[string] {
	return data.ToNullable(s, true, asked)
}
