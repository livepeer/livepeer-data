package usage

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	livepeer "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
)

type Metric struct {
	TimeInterval *int64 `json:"TimeInterval,omitempty"`

	// breakdown fields
	UserID    string                `json:"UserID,omitempty"`
	CreatorID data.Nullable[string] `json:"CreatorID,omitempty"`

	// metric data
	DeliveryUsageMins data.Nullable[float64] `json:"DeliveryUsageMins,omitempty"`
	TotalUsageMins    data.Nullable[float64] `json:"TotalUsageMins,omitempty"`
	StorageUsageMins  data.Nullable[float64] `json:"StorageUsageMins,omitempty"`
}

type Client struct {
	opts     ClientOptions
	lp       *livepeer.Client
	bigquery BigQuery
}

type ClientOptions struct {
	Livepeer livepeer.ClientOptions
	BigQueryOptions
}

func NewClient(opts ClientOptions) (*Client, error) {
	lp := livepeer.NewAPIClient(opts.Livepeer)
	bqOpts := opts.BigQueryOptions

	bigquery, err := NewBigQuery(bqOpts)
	if err != nil {
		return nil, fmt.Errorf("error creating bigquery client: %w", err)
	}

	return &Client{opts, lp, bigquery}, nil
}

func (c *Client) QuerySummary(ctx context.Context, userID string, spec QuerySpec) ([]Metric, error) {
	summary, err := c.bigquery.QueryUsageSummary(ctx, userID, spec)
	if err != nil {
		return nil, err
	}

	metrics := usageSummaryToMetrics(summary, spec)
	return metrics, nil
}

func usageSummaryToMetrics(rows []UsageSummaryRow, spec QuerySpec) []Metric {
	metrics := make([]Metric, len(rows))
	for i, row := range rows {
		m := Metric{
			UserID:            row.UserID,
			CreatorID:         toStringPtr(row.CreatorID, spec.hasBreakdownBy("creatorId")),
			DeliveryUsageMins: toFloat64Ptr(row.DeliveryUsageMins, true),
			TotalUsageMins:    toFloat64Ptr(row.TotalUsageMins, true),
			StorageUsageMins:  toFloat64Ptr(row.StorageUsageMins, true),
		}

		if !row.TimeInterval.IsZero() {
			timestamp := row.TimeInterval.UnixMilli()
			m.TimeInterval = &timestamp
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
	if usageBreakdownFields[e] == "" {
		panic(fmt.Sprintf("unknown breakdown field %q", e))
	}

	for _, a := range q.BreakdownBy {
		if strings.EqualFold(a, e) {
			return true
		}
	}
	return false
}

func (c *Client) QuerySummaryWithTimestep(ctx context.Context, userID string, spec QuerySpec) ([]Metric, error) {
	summary, err := c.bigquery.QueryUsageSummaryWithTimestep(ctx, userID, spec)
	if err != nil {
		return nil, err
	}

	metrics := usageSummaryToMetrics(summary, spec)
	return metrics, nil
}

func (c *Client) QueryTotalSummary(ctx context.Context, spec FromToQuerySpec) ([]TotalUsageSummaryRow, error) {
	summary, err := c.bigquery.QueryTotalUsageSummary(ctx, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}

func (c *Client) QueryActiveUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]ActiveUsersSummaryRow, error) {
	summary, err := c.bigquery.QueryActiveUsersUsageSummary(ctx, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}
