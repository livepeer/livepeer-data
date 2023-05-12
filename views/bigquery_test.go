package views

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func TestCallsBigQueryClient(t *testing.T) {
	require := require.New(t)

	var receivedQuery string
	stub := &stubBigqueryClient{
		query: func(q string) *bigquery.Query {
			receivedQuery = q
			return &bigquery.Query{QueryConfig: bigquery.QueryConfig{DryRun: true}}
		},
	}
	bq := &bigqueryHandler{client: stub, opts: BigQueryOptions{ViewershipEventsTable: "bq_events"}}

	_, err := bq.QueryViewsEvents(context.Background(), QuerySpec{})
	require.ErrorContains(err, "dry-run")

	require.Equal("SELECT countif(play_intent) as view_count, sum(playtime_ms) / 60000.0 as playtime_mins, playback_id "+
		"FROM bq_events WHERE account_id = ? GROUP BY playback_id LIMIT 10001", receivedQuery)
}

func TestBuildViewsEventsQuery(t *testing.T) {
	require := require.New(t)

	table := "bq_events"
	spec := QuerySpec{
		Filter: QueryFilter{
			PlaybackID: "p1",
			UserID:     "u1",
			CreatorID:  "c1",
		},
		TimeStep:    "day",
		BreakdownBy: []string{"deviceType", "continent"},
	}

	sql, args, err := buildViewsEventsQuery(table, spec)
	require.NoError(err)

	require.Regexp("FROM bq_events", sql)
	require.Regexp("(WHERE|AND) account_id = ?", sql)
	require.Regexp("(WHERE|AND) creator_id_type = ?", sql)
	require.Regexp("(WHERE|AND) creator_id = ?", sql)
	for _, field := range []string{"p1", "u1", "c1"} {
		require.Contains(args, field)
	}
	require.Regexp("timestamp_trunc\\(time, day\\) as time_interval", sql)
	require.Regexp("GROUP BY playback_id, time_interval, device_type, playback_continent_name", sql)
}

func TestBuildViewsSummaryByPlaybackIDQuery(t *testing.T) {
	require := require.New(t)

	table := "bq_summary"
	playbackID := "p1"

	sql, args, err := buildViewsSummaryQuery(table, playbackID)
	require.NoError(err)

	require.Regexp("FROM bq_summary", sql)
	require.Regexp("(WHERE|AND) playback_id = ?", sql)
	require.Contains(args, playbackID)
	require.Regexp("GROUP BY.+playback_id", sql)
}

func TestBuildViewsSummaryByDStorageUrlQuery(t *testing.T) {
	require := require.New(t)

	table := "bq_summary"
	playbackID := "ipfs://bafoo"

	sql, args, err := buildViewsSummaryQuery(table, playbackID)
	require.NoError(err)

	require.Regexp("FROM bq_summary", sql)
	require.Regexp("(WHERE|AND) d_storage_url = ?", sql)
	require.Contains(args, playbackID)
	require.Regexp("GROUP BY.+d_storage_url", sql)
}

func TestBuildViewsSummaryByDStorageIdQuery(t *testing.T) {
	require := require.New(t)

	table := "bq_summary"
	playbackID := "bafybeibojaukglnsdfh3fplj3nz4hozis7xjsad534zwpzwhvrb7dnolri"

	sql, args, err := buildViewsSummaryQuery(table, playbackID)
	require.NoError(err)

	require.Regexp("FROM bq_summary", sql)
	require.Regexp("(WHERE|AND) d_storage_url = ?", sql)
	require.Contains(args, "ipfs://"+playbackID)
	require.Regexp("GROUP BY.+d_storage_url", sql)
}

type stubBigqueryClient struct {
	query func(string) *bigquery.Query
}

func (s *stubBigqueryClient) Query(q string) *bigquery.Query {
	if s.query == nil {
		panic("not implemented")
	}
	return s.query(q)
}
