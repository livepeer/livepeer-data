package views

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	PlaytimeMins     float64                `json:"playtimeMins"`
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

	// We never want to return `null` for the legacy view count, so we don't use
	// the regular nullable creation.
	legacyViewCount := int64(0)
	if summary.LegacyViewCount.Valid {
		legacyViewCount = summary.LegacyViewCount.Int64
	}

	return &Metric{
		PlaybackID:      toStringPtr(summary.PlaybackID, summary.PlaybackID.Valid),
		DStorageURL:     toStringPtr(summary.DStorageURL, summary.DStorageURL.Valid),
		ViewCount:       summary.ViewCount,
		LegacyViewCount: data.ToNullable[int64](legacyViewCount, true, true),
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
			if spec.Filter.UserID != asset.UserID {
				return nil, fmt.Errorf("error getting asset: verify that asset exists and you are using proper credentials")
			}
		}
	} else if streamID != "" {
		var stream *livepeer.Stream

		stream, err = c.lp.GetStream(streamID, false)
		if stream != nil {
			spec.Filter.PlaybackID = stream.PlaybackID
			if spec.Filter.UserID != stream.UserID {
				return nil, fmt.Errorf("error getting asset: verify that asset exists and you are using proper credentials")
			}
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

	metrics := viewershipEventsToMetrics(rows, spec)
	return metrics, nil
}

func viewershipEventsToMetrics(rows []ViewershipEventRow, spec QuerySpec) []Metric {
	metrics := make([]Metric, len(rows))
	for i, row := range rows {
		m := Metric{
			CreatorID:        toStringPtr(row.CreatorID, spec.hasBreakdownBy("creatorId")),
			ViewerID:         toStringPtr(row.ViewerID, spec.hasBreakdownBy("viewerId")),
			PlaybackID:       toStringPtr(row.PlaybackID, spec.hasBreakdownBy("playbackId")),
			DStorageURL:      toStringPtr(row.DStorageURL, spec.hasBreakdownBy("dStorageUrl")),
			Device:           toStringPtr(row.Device, spec.hasBreakdownBy("device")),
			OS:               toStringPtr(row.OS, spec.hasBreakdownBy("os")),
			Browser:          toStringPtr(row.Browser, spec.hasBreakdownBy("browser")),
			Continent:        toStringPtr(row.Continent, spec.hasBreakdownBy("continent")),
			Country:          toStringPtr(row.Country, spec.hasBreakdownBy("country")),
			Subdivision:      toStringPtr(row.Subdivision, spec.hasBreakdownBy("subdivision")),
			TimeZone:         toStringPtr(row.TimeZone, spec.hasBreakdownBy("timezone")),
			GeoHash:          toStringPtr(row.GeoHash, spec.hasBreakdownBy("geohash")),
			ViewCount:        row.ViewCount,
			PlaytimeMins:     row.PlaytimeMins,
			TtffMs:           toFloat64Ptr(row.TtffMs, spec.Detailed),
			RebufferRatio:    toFloat64Ptr(row.RebufferRatio, spec.Detailed),
			ErrorRate:        toFloat64Ptr(row.ErrorRate, spec.Detailed),
			ExitsBeforeStart: toFloat64Ptr(row.ExitsBeforeStart, spec.Detailed),
		}

		if !row.TimeInterval.IsZero() {
			timestamp := row.TimeInterval.UnixMilli()
			m.Timestamp = &timestamp
		}

		metrics[i] = m
	}
	return metrics
}

func toFloat64Ptr(bqFloat bigquery.NullFloat64, asked bool) data.Nullable[float64] {
	return data.ToNullable(bqFloat.Float64, bqFloat.Valid, asked)
}

func toStringPtr(bqStr bigquery.NullString, asked bool) data.Nullable[string] {
	return data.ToNullable(bqStr.StringVal, bqStr.Valid, asked)
}

func (q *QuerySpec) hasBreakdownBy(e string) bool {
	// callers always set `e` as a string literal so we can panic if it's not valid
	if viewershipBreakdownFields[e] == "" {
		panic(fmt.Sprintf("unknown breakdown field %q", e))
	}

	for _, a := range q.BreakdownBy {
		if strings.EqualFold(a, e) {
			return true
		}
	}
	return false
}
