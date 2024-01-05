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

	require.Equal("SELECT countif(play_intent) as view_count, ifnull(sum(playtime_ms), 0) / 60000.0 as playtime_mins "+
		"FROM bq_events WHERE account_id = ? LIMIT 10001", receivedQuery)
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

	// SELECT
	require.Regexp("timestamp_trunc\\(time, day\\) as time_interval", sql)
	require.Regexp("FROM bq_events", sql)

	// WHERE
	require.Regexp("(WHERE|AND) account_id = ?", sql)
	require.Contains(args, "u1")
	require.Regexp("(WHERE|AND) creator_id_type = ?", sql)
	require.Contains(args, "unverified")
	require.Regexp("(WHERE|AND) creator_id = ?", sql)
	require.Contains(args, "c1")
	require.Regexp("(WHERE|AND) playback_id = ?", sql)
	require.Contains(args, "p1")

	// GROUP BY
	require.Regexp("GROUP BY playback_id, time_interval, device_type, playback_continent_name", sql)
}

func TestBuildViewsByPlaybackIDQuery(t *testing.T) {
	require := require.New(t)

	testQuery := func(pidFilter string, breakdownBy []string) {
		spec := QuerySpec{
			Filter: QueryFilter{
				PlaybackID: pidFilter,
				UserID:     "u1",
			},
			TimeStep:    "hour",
			BreakdownBy: breakdownBy,
		}

		sql, args, err := buildViewsEventsQuery("bq_events", spec)
		require.NoError(err)

		// SELECT
		require.Regexp("timestamp_trunc\\(time, hour\\) as time_interval", sql)
		require.Regexp("FROM bq_events", sql)

		// WHERE
		require.Regexp("(WHERE|AND) account_id = ?", sql)
		require.Contains(args, "u1")
		if pidFilter != "" {
			require.Regexp("(WHERE|AND) playback_id = ?", sql)
			require.Contains(args, pidFilter)
		}

		// GROUP BY
		require.Regexp(`GROUP BY (playback_id, )?time_interval`, sql)
		if pidFilter != "" || len(breakdownBy) > 0 {
			require.Regexp(`GROUP BY (time_interval, )?playback_id`, sql)
		}
	}

	testQuery("", nil)
	testQuery("p1", nil)
	testQuery("", []string{"playbackId"})
	testQuery("p1", []string{"playbackId"})
}

func TestBuildViewsByDStorageURL(t *testing.T) {
	require := require.New(t)

	txID := `ZGNiYWBfXl1cW1pZWFdWVVRTUlFQT05NTEtKSUhHRkU`

	testQuery := func(pidFilter string, breakdownBy []string) {
		spec := QuerySpec{
			Filter: QueryFilter{
				PlaybackID: pidFilter,
				UserID:     "u1",
			},
			TimeStep:    "hour",
			BreakdownBy: breakdownBy,
		}

		sql, args, err := buildViewsEventsQuery("bq_events", spec)
		require.NoError(err)

		// SELECT
		require.Regexp("timestamp_trunc\\(time, hour\\) as time_interval", sql)
		require.Regexp("FROM bq_events", sql)

		// WHERE
		require.Regexp("(WHERE|AND) account_id = ?", sql)
		require.Contains(args, "u1")
		if pidFilter != "" {
			require.Regexp("(WHERE|AND) d_storage_url = ?", sql)
			require.Contains(args, "ar://"+pidFilter)
		}

		// GROUP BY
		require.Regexp(`GROUP BY (d_storage_url, )?time_interval`, sql)
		if pidFilter != "" || len(breakdownBy) > 0 {
			require.Regexp(`GROUP BY (time_interval, )?d_storage_url`, sql)
		}
	}

	testQuery("", nil)
	testQuery(txID, nil)
	testQuery("", []string{"dStorageUrl"})
	testQuery(txID, []string{"dStorageUrl"})
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
