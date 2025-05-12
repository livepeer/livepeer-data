package ai

import (
	"context"
	"fmt"

	"github.com/livepeer/livepeer-data/prometheus"
	promClient "github.com/prometheus/client_golang/api"
)

type Client struct {
	prom *prometheus.Prometheus
}

func NewClient(promConfig promClient.Config) (*Client, error) {
	prom, err := prometheus.NewPrometheus(promConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus client: %w", err)
	}

	return &Client{prom}, nil
}

func (c *Client) QueryAICapacity(ctx context.Context, regions, nodeID, regionsExclude string) (prometheus.AICapacity, error) {
	return c.prom.QueryAICapacity(ctx, regions, nodeID, regionsExclude)
}
