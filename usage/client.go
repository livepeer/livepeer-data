package usage

import (
	"context"
	"fmt"

	livepeer "github.com/livepeer/go-api-client"
)

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

func (c *Client) QuerySummary(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*UsageSummaryRow, error) {
	summary, err := c.bigquery.QueryUsageSummary(ctx, userID, creatorID, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}

func (c *Client) QuerySummaryWithTimestep(ctx context.Context, userID string, creatorID string, spec QuerySpec) (*[]UsageSummaryRow, error) {
	summary, err := c.bigquery.QueryUsageSummaryWithTimestep(ctx, userID, creatorID, spec)
	if err != nil {
		return nil, err
	}

	return summary, nil
}
