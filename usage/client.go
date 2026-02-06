package usage

import (
	"context"
	"fmt"

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
	billing  BigQuery // backend for billing queries (ClickHouse or BigQuery)
	bigquery BigQuery // always BigQuery, used for non-billing queries like total usage
}

type ClientOptions struct {
	Livepeer livepeer.ClientOptions
	BigQueryOptions
	Clickhouse ClickhouseOptions
}

func NewClient(opts ClientOptions) (*Client, error) {
	lp := livepeer.NewAPIClient(opts.Livepeer)

	// Always create BigQuery client for non-billing queries (total usage / explorer_day_data)
	var bq BigQuery
	if opts.BigQueryCredentialsJSON != "" {
		var err error
		bq, err = NewBigQuery(opts.BigQueryOptions)
		if err != nil {
			return nil, fmt.Errorf("error creating bigquery client: %w", err)
		}
	}

	// Use ClickHouse for billing queries if configured, otherwise BigQuery
	billing := bq
	if opts.Clickhouse.Addr != "" {
		ch, err := NewClickhouse(opts.Clickhouse)
		if err != nil {
			return nil, fmt.Errorf("error creating clickhouse client: %w", err)
		}
		billing = ch
	}

	return &Client{opts, lp, billing, bq}, nil
}

func (c *Client) QuerySummary(ctx context.Context, spec QuerySpec) (*Metric, error) {
	summary, err := c.billing.QueryUsageSummary(ctx, spec)
	if err != nil {
		return nil, err
	}

	metric := usageSummaryToMetric(summary, spec)
	return metric, nil
}

func usageSummaryToMetric(row *UsageSummaryRow, spec QuerySpec) *Metric {
	inclCreatorID := spec.Filter.CreatorID != "" || spec.hasBreakdownBy("creatorId")
	m := &Metric{
		UserID:            row.UserID,
		CreatorID:         toStringPtr(row.CreatorID, inclCreatorID),
		DeliveryUsageMins: toFloat64Ptr(row.DeliveryUsageMins, true),
		TotalUsageMins:    toFloat64Ptr(row.TotalUsageMins, true),
		StorageUsageMins:  toFloat64Ptr(row.StorageUsageMins, true),
	}

	if !row.TimeInterval.IsZero() {
		timestamp := row.TimeInterval.UnixMilli()
		m.TimeInterval = &timestamp
	}

	return m
}

func toFloat64Ptr(bqFloat bigquery.NullFloat64, asked bool) data.Nullable[float64] {
	return data.ToNullable(bqFloat.Float64, bqFloat.Valid, asked)
}

func toStringPtr(bqStr bigquery.NullString, asked bool) data.Nullable[string] {
	return data.ToNullable(bqStr.StringVal, bqStr.Valid, asked)
}

func (c *Client) QuerySummaryWithBreakdown(ctx context.Context, spec QuerySpec) ([]Metric, error) {
	summary, err := c.billing.QueryUsageSummaryWithBreakdown(ctx, spec)
	if err != nil {
		return nil, err
	}

	metrics := make([]Metric, len(summary))
	for i, row := range summary {
		metrics[i] = *usageSummaryToMetric(&row, spec)
	}
	return metrics, nil
}

func (c *Client) QueryTotalSummary(ctx context.Context, spec FromToQuerySpec) ([]TotalUsageSummaryRow, error) {
	// Total usage (explorer_day_data) always goes through BigQuery
	if c.bigquery == nil {
		return nil, fmt.Errorf("bigquery client not configured, required for total usage queries")
	}
	summary, err := c.bigquery.QueryTotalUsageSummary(ctx, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}

func (c *Client) QueryActiveUsageSummary(ctx context.Context, spec FromToQuerySpec) ([]ActiveUsersSummaryRow, error) {
	summary, err := c.billing.QueryActiveUsersUsageSummary(ctx, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}
