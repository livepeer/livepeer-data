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
	ID         string `json:"id"`
	StartViews int64  `json:"startViews"`
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
		// TODO: Remove this testing hack, only allow the query for existing assets
		// return nil, errors.New("asset not found")
		asset = &livepeer.Asset{PlaybackID: id}
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset: %w", err)
	}

	startViews, err := c.doQueryStartViews(ctx, asset.PlaybackID)
	if err != nil {
		return nil, fmt.Errorf("error querying start views: %w", err)
	}

	return []TotalViews{{
		ID:         asset.PlaybackID,
		StartViews: startViews,
	}}, nil
}

func (c *Client) doQueryStartViews(ctx context.Context, playbackID string) (int64, error) {
	value, warn, err := c.prom.Query(ctx, startViewsQuery(playbackID), time.Time{})
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

func startViewsQuery(playbackID string) string {
	return fmt.Sprintf(
		`sum(increase(mist_playux_count{strm=~"video(rec)?\\+%s"} [1y]))`,
		playbackID,
	)
}
