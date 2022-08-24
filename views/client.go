package views

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	livepeer "github.com/livepeer/go-api-client"
	promClient "github.com/prometheus/client_golang/api"
	prometheus "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type TotalViews struct {
	ID    string `json:"id"`
	Start int64  `json:"start"`
}

type ClientOptions struct {
	Prometheus promClient.Config
	Livepeer   livepeer.ClientOptions
}

type Client struct {
	prom prometheus.API
	lp   *livepeer.Client
}

func NewClient(opts ClientOptions) (*Client, error) {
	client, err := promClient.NewClient(opts.Prometheus)
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus client: %w", err)
	}
	prom := prometheus.NewAPI(client)
	lp := livepeer.NewAPIClient(opts.Livepeer)
	return &Client{prom, lp}, nil
}

func (c *Client) GetTotalViews(ctx context.Context, id string) ([]TotalViews, error) {
	asset, err := c.lp.GetAsset(id)
	if errors.Is(err, livepeer.ErrNotExists) {
		asset, err = c.lp.GetAssetByPlaybackID(id, false)
	}
	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, errors.New("asset not found")
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset: %w", err)
	}

	value, warn, err := c.prom.Query(ctx, startViewsQuery(asset), time.Time{})
	if len(warn) > 0 {
		glog.Warningf("Prometheus query warnings: %q", warn)
	}
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	if value.Type() != model.ValScalar {
		return nil, fmt.Errorf("unexpected value type: %s", value.Type())
	}

	return []TotalViews{{
		ID:    asset.PlaybackID,
		Start: int64(value.(*model.Scalar).Value),
	}}, nil
}

func startViewsQuery(asset *livepeer.Asset) string {
	return fmt.Sprintf(
		`sum(increase(mist_viewcount{stream=~"video(rec)?\\+%s"} [1y]))`,
		asset.PlaybackID,
	)
}
