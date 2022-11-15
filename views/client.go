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

var ErrAssetNotFound = errors.New("asset not found")
var ErrStreamNotFound = errors.New("stream not found")

type TotalViews struct {
	ID         string `json:"id"`
	StartViews int64  `json:"startViews"`
}

type RealTimeViews struct {
	ID    string `json:"id"`
	Views int64  `json:"views"`
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
		return nil, ErrAssetNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting asset: %w", err)
	}

	startViews, err := c.doQueryStartViews(ctx, asset)
	if err != nil {
		return nil, fmt.Errorf("error querying start views: %w", err)
	}

	return []TotalViews{{
		ID:         asset.PlaybackID,
		StartViews: startViews,
	}}, nil
}

func (c *Client) GetRealTimeViews(ctx context.Context, id string) ([]RealTimeViews, error) {
	stream, err := c.lp.GetStream(id, false)
	if errors.Is(err, livepeer.ErrNotExists) {
		return nil, ErrStreamNotFound
	} else if err != nil {
		return nil, fmt.Errorf("error getting stream: %w", err)
	}

	realTimeViews, err := c.doQueryRealTimeViews(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("error querying real time views: %w", err)
	}

	return []RealTimeViews{{
		ID:    stream.PlaybackID,
		Views: realTimeViews,
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

func (c *Client) doQueryRealTimeViews(ctx context.Context, stream *livepeer.Stream) (int64, error) {
	query := realTimeViewsQuery(stream.PlaybackID)
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

func realTimeViewsQuery(playbackID string) string {
	return fmt.Sprintf(
		`sum(mist_sessions{catalyst="true", sessType="viewers", stream=~"video+%s"}[1m] )`,
		playbackID,
	)
}
