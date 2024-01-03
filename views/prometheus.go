package views

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	livepeer "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	promClient "github.com/prometheus/client_golang/api"
	prometheus "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type TotalViews struct {
	ID         string               `json:"id"`
	StartViews data.Nullable[int64] `json:"startViews"`
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
