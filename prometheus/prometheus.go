package prometheus

import (
	"context"
	"fmt"
	"strings"
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
	IdleContainers int64 `json:"idleContainers"`
	ActiveStreams  int64 `json:"activeStreams"`
}

func (c *Prometheus) QueryAICapacity(ctx context.Context, regions, nodeID, regionsExclude, models, additionalFilters string) (AICapacity, error) {
	regionFilter, nodeIDFilter, modelFilter := "", "", ""
	if regions != "" {
		regionFilter = fmt.Sprintf(`, region=~"%s"`, strings.Replace(regions, ",", "|", -1))
	}
	if nodeID != "" {
		nodeIDFilter = fmt.Sprintf(`, node_id="%s"`, nodeID)
	}
	if models != "" {
		modelFilter = fmt.Sprintf(`, model_name=~"%s"`, strings.Replace(models, ",", "|", -1))
	}
	regionsExcludeFilter := `, region!~".*secondary", region!~"1legion.*"`
	if regionsExclude != "" {
		regionsExcludeFilter = fmt.Sprintf(`, region!~"%s"`, strings.Replace(regionsExclude, ",", "|", -1))
	}
	filters := fmt.Sprintf(`{orchestrator_uri!=""%s%s%s%s%s}`, regionsExcludeFilter, regionFilter, nodeIDFilter, additionalFilters, modelFilter)

	query := fmt.Sprintf(`sum(max by(orchestrator_uri) (livepeer_ai_container_idle%s))`, filters)

	idle, err := c.queryInt64(ctx, query)
	if err != nil {
		return AICapacity{}, err
	}

	if models != "" {
		modelFilter = fmt.Sprintf(`, pipeline=~"%s"`, strings.Replace(models, ",", "|", -1))
	}
	streamsFilters := fmt.Sprintf(`{livepeer_node_type=~".*-livepeer-ai-gateway.*"%s%s%s%s}`, regionsExcludeFilter, regionFilter, additionalFilters, modelFilter)

	active, err := c.queryInt64(ctx, fmt.Sprintf(`sum(max by(region, node_id) (livepeer_ai_current_live_pipelines%s))`, streamsFilters))
	if err != nil {
		return AICapacity{}, err
	}

	return AICapacity{
		IdleContainers: idle,
		ActiveStreams:  active,
	}, nil
}
