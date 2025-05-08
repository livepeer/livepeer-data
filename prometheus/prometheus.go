package prometheus

import (
	"context"
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

type Prometheus struct {
	api prometheus.API
}

func NewPrometheus(config promClient.Config) (*Prometheus, error) {
	client, err := promClient.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus client: %w", err)
	}
	api := prometheus.NewAPI(client)

	return &Prometheus{api}, nil
}

func (c *Prometheus) QueryStartViews(ctx context.Context, asset *livepeer.Asset) (int64, error) {
	query := startViewsQuery(asset.PlaybackID, asset.PlaybackRecordingID)
	return c.queryInt64(ctx, query)
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

func (c *Prometheus) QueryRealtimeViews(ctx context.Context, userId string) (int64, error) {
	query := fmt.Sprintf(`sum(mist_sessions{sessType="viewers", user_id="%s"})`, userId)
	return c.queryInt64(ctx, query)
}

func (c *Prometheus) queryInt64(ctx context.Context, query string) (int64, error) {
	value, warn, err := c.api.Query(ctx, query, time.Time{})
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

type AICapacity struct {
	TotalContainers int64 `json:"totalContainers"`
	InUseContainers int64 `json:"inUseContainers"`
	IdleContainers  int64 `json:"idleContainers"`
}

func (c *Prometheus) QueryAICapacity(ctx context.Context, region, nodeID string) (AICapacity, error) {
	regionFilter, nodeIDFilter := "", ""
	if region != "" {
		regionFilter = fmt.Sprintf(`, region="%s"`, region)
	}
	if nodeID != "" {
		nodeIDFilter = fmt.Sprintf(`, node_id="%s"`, nodeID)
	}
	query := fmt.Sprintf(`sum(livepeer_ai_container_idle{job="orchestrator", region!~".*secondary", region!~"1legion.*"%s%s})`, regionFilter, nodeIDFilter)

	idle, err := c.queryInt64(ctx, query)
	if err != nil {
		return AICapacity{}, err
	}

	query = fmt.Sprintf(`sum(livepeer_ai_container_in_use{job="orchestrator", region!~".*secondary", region!~"1legion.*"%s%s})`, regionFilter, nodeIDFilter)

	inUse, err := c.queryInt64(ctx, query)
	if err != nil {
		return AICapacity{}, err
	}

	return AICapacity{
		TotalContainers: idle + inUse,
		InUseContainers: inUse,
		IdleContainers:  idle,
	}, nil
}
