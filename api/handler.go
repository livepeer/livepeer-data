package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/ai"
	"github.com/livepeer/livepeer-data/health"
	"github.com/livepeer/livepeer-data/metrics"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/jsse"
	"github.com/livepeer/livepeer-data/prometheus"
	"github.com/livepeer/livepeer-data/usage"
	"github.com/livepeer/livepeer-data/views"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cache "github.com/victorspringer/http-cache"
	"github.com/victorspringer/http-cache/adapter/memory"
)

const (
	sseRetryBackoff = 10 * time.Second
	ssePingDelay    = 20 * time.Second
	sseBufferSize   = 128

	streamIDParam   = "streamId"
	assetIDParam    = "assetId"
	playbackIDParam = "playbackId"
)

var httpCache *cache.Client

func init() {
	memcached, err := memory.NewAdapter(
		memory.AdapterWithAlgorithm(memory.LRU),
		memory.AdapterWithCapacity(2000),
	)
	if err != nil {
		panic(err)
	}

	httpCache, err = cache.NewClient(
		cache.ClientWithAdapter(memcached),
		cache.ClientWithTTL(5*time.Minute),
	)
	if err != nil {
		panic(err)
	}
}

type APIHandlerOptions struct {
	ServerName, APIRoot, AuthURL  string
	RegionalHostFormat, OwnRegion string
	Prometheus                    bool
}

type apiHandler struct {
	opts      APIHandlerOptions
	serverCtx context.Context
	core      *health.Core
	views     *views.Client
	usage     *usage.Client
	ai        *ai.Client
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, healthcore *health.Core, views *views.Client, usage *usage.Client, ai *ai.Client) http.Handler {
	handler := &apiHandler{opts, serverCtx, healthcore, views, usage, ai}

	router := chi.NewRouter()

	// don't use middlewares for the system routes
	router.Get("/_healthz", handler.healthcheck)
	if opts.Prometheus {
		router.Method("GET", "/metrics", promhttp.Handler())
	}

	router.Route(opts.APIRoot, func(router chi.Router) {
		router.Use(chimiddleware.Logger)
		router.Use(chimiddleware.NewCompressor(5, "application/json").Handler)
		router.Use(handler.cors())

		router.Mount(`/stream/{`+streamIDParam+`}`, handler.streamHealthHandler())
		router.Mount("/views", handler.viewershipHandler())
		router.Mount("/usage", handler.usageHandler())
		router.Mount("/ai", handler.aiHandler())
	})

	return router
}

// {streamId} variable must be set in the request context
func (h *apiHandler) streamHealthHandler() chi.Router {
	healthcore, opts := h.core, h.opts

	router := chi.NewRouter()
	if opts.AuthURL != "" {
		router.Use(authorization(opts.AuthURL))
	}
	router.Use(
		streamStatus(healthcore),
		regionProxy(opts.RegionalHostFormat, opts.OwnRegion))

	h.withMetrics(router, "get_stream_health").
		MethodFunc("GET", "/health", h.getStreamHealth)
	h.withMetrics(router, "stream_health_events").
		MethodFunc("GET", "/events", h.subscribeEvents)

	return router
}

func notImplemented() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		respondError(rw, http.StatusNotImplemented, errors.New("bigquery is unavailable"))
	})
}

func (h *apiHandler) viewershipHandler() chi.Router {
	opts := h.opts

	router := chi.NewRouter()
	if h.views == nil {
		router.Handle("/*", notImplemented())
		return router
	}
	if opts.AuthURL != "" {
		router.Use(authorization(opts.AuthURL))
	}

	// TODO: Remove this deprecated endpoint once we know no one is using it
	h.withMetrics(router, "get_total_views").
		With(h.cache(false)).
		MethodFunc("GET", fmt.Sprintf(`/{%s}/total`, assetIDParam), h.getTotalViews)

	// realtime viewership server side (internal-only)
	h.withMetrics(router, "query_realtime_server_viewership").
		MethodFunc("GET", "/internal/server/now", h.queryRealtimeServerViewership())

	// total views public API
	h.withMetrics(router, "query_total_viewership").
		With(h.cache(false)).
		MethodFunc("GET", fmt.Sprintf(`/query/total/{%s}`, playbackIDParam), h.queryTotalViewership)
	// creator views API, requires assetId or streamId on the query-string
	h.withMetrics(router, "query_creator_viewership").
		With(h.cache(true)).
		With(ensureIsCreatorQuery).
		MethodFunc("GET", `/query/creator`, h.queryViewership(false))
	// full application views API, gets access to all metrics and filters
	h.withMetrics(router, "query_application_viewership").
		With(h.cache(true)).
		MethodFunc("GET", `/query`, h.queryViewership(true))
	// realtime viewership
	h.withMetrics(router, "query_realtime_viewership").
		MethodFunc("GET", `/now`, h.queryRealtimeViewership())
	h.withMetrics(router, "query_timeseries_realtime_viewership").
		MethodFunc("GET", `/internal/timeSeries`, h.queryTimeSeriesRealtimeViewership())

	return router
}

func (h *apiHandler) usageHandler() chi.Router {
	opts := h.opts

	router := chi.NewRouter()
	if h.usage == nil {
		router.Handle("/*", notImplemented())
		return router
	}
	if opts.AuthURL != "" {
		router.Use(authorization(opts.AuthURL))
	}

	h.withMetrics(router, "query_usage").
		With(h.cache(true)).
		MethodFunc("GET", `/query`, h.queryUsage())

	h.withMetrics(router, "query_total_usage").
		With(h.cache(true)).
		MethodFunc("GET", `/query/total`, h.queryTotalUsage())

	h.withMetrics(router, "query_active_users").
		With(h.cache(true)).
		MethodFunc("GET", `/query/active`, h.queryActiveUsersUsage())

	return router
}

func (h *apiHandler) aiHandler() chi.Router {
	router := chi.NewRouter()
	if h.ai == nil {
		router.Handle("/*", notImplemented())
		return router
	}

	h.withMetrics(router, "query_ai_capacity").
		MethodFunc("GET", `/capacity`, h.queryAICapacity())

	return router
}

func (h *apiHandler) withMetrics(router chi.Router, name string) chi.Router {
	if !h.opts.Prometheus {
		return router
	}
	return router.With(func(handler http.Handler) http.Handler {
		return metrics.ObservedHandler(name, handler)
	})
}

func (h *apiHandler) cache(varyAuth bool) middleware {
	return func(next http.Handler) http.Handler {
		next = httpCache.Middleware(next)

		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Cache-Control", "public, max-age=60, s-maxage=300, stale-while-revalidate=3600, stale-if-error=86400")

			if varyAuth {
				rw.Header().Add("Vary", "Authorization")

				// cache lib uses only URL as key so add auth header to the query-string
				query := r.URL.Query()
				query.Add("auth-header", r.Header.Get("Authorization"))
				r.URL.RawQuery = query.Encode()
			}

			next.ServeHTTP(rw, r)
		})
	}
}

func (h *apiHandler) cors() middleware {
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		if h.opts.ServerName != "" {
			rw.Header().Set("Server", h.opts.ServerName)
		}
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Header().Set("Access-Control-Allow-Headers", "*")
		if origin := r.Header.Get("Origin"); origin != "" {
			rw.Header().Set("Access-Control-Allow-Origin", origin)
			rw.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		next.ServeHTTP(rw, r)
	})
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	if h.core != nil && !h.core.IsHealthy() {
		status = http.StatusServiceUnavailable
	}
	rw.WriteHeader(status)
}

func (h *apiHandler) queryTotalViewership(rw http.ResponseWriter, r *http.Request) {
	playbackID := apiParam(r, playbackIDParam)

	metric, err := h.views.QuerySummary(r.Context(), playbackID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	if metric == nil {
		metric = &views.Metric{
			PlaybackID: data.ToNullable[string](playbackID, true, true),
		}
		if dStorageURL := views.ToDStorageURL(playbackID); dStorageURL != "" {
			metric = &views.Metric{
				DStorageURL: data.ToNullable[string](dStorageURL, true, true),
			}
		}
	}

	respondJson(rw, http.StatusOK, metric)
}

func ensureIsCreatorQuery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// disallow querying creator metrics with a playback ID
		qs := r.URL.Query()
		qs.Del("playbackId")
		r.URL.RawQuery = qs.Encode()

		hasAsset, hasStream := qs.Get("assetId") != "", qs.Get("streamId") != ""
		if hasAsset == hasStream {
			respondError(rw, http.StatusBadRequest, errors.New("must provide exactly 1 of assetId or streamId for creator query"))
			return
		}

		next.ServeHTTP(rw, r)
	})
}

func (h *apiHandler) queryViewership(detailed bool) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		querySpec, httpErroCode, errs := h.resolveViewershipQuerySpec(r)
		if len(errs) > 0 {
			respondError(rw, httpErroCode, errs...)
			return
		}
		querySpec.Detailed = detailed

		metrics, err := h.views.QueryEvents(r.Context(), querySpec)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) queryUsage() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var (
			from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
			to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
		)
		if errs := nonNilErrs(err1, err2); len(errs) > 0 {
			respondError(rw, http.StatusBadRequest, errs...)
			return
		}

		userId := callerUserId(r)
		if userId == "" {
			respondError(rw, http.StatusInternalServerError, errors.New("request not authenticated"))
			return
		}

		if qs := r.URL.Query(); qs.Has("userId") {
			if !isCallerAdmin(r) {
				respondError(rw, http.StatusForbidden, errors.New("only admins can query usage for other users"))
				return
			}
			userId = qs.Get("userId")
		}

		qs := r.URL.Query()

		query := usage.QuerySpec{
			From:     from,
			To:       to,
			TimeStep: qs.Get("timeStep"),
			Filter: usage.QueryFilter{
				UserID:    userId,
				CreatorID: qs.Get("creatorId"),
			},
			BreakdownBy: qs["breakdownBy[]"],
		}

		if !query.HasAnyBreakdown() {
			usage, err := h.usage.QuerySummary(r.Context(), query)
			if err != nil {
				respondError(rw, http.StatusInternalServerError, err)
				return
			}

			respondJson(rw, http.StatusOK, usage)
		} else {
			usage, err := h.usage.QuerySummaryWithBreakdown(r.Context(), query)
			if err != nil {
				respondError(rw, http.StatusInternalServerError, err)
				return
			}

			respondJson(rw, http.StatusOK, usage)
		}

	}
}

func (h *apiHandler) queryTotalUsage() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var (
			from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
			to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
		)
		if errs := nonNilErrs(err1, err2); len(errs) > 0 {
			respondError(rw, http.StatusBadRequest, errs...)
			return
		}

		if userId := callerUserId(r); userId == "" {
			respondError(rw, http.StatusInternalServerError, errors.New("request not authenticated"))
			return
		}

		if !isCallerAdmin(r) {
			respondError(rw, http.StatusForbidden, errors.New("only admins can query total usage"))
			return
		}

		query := usage.FromToQuerySpec{
			From: from,
			To:   to,
		}

		usage, err := h.usage.QueryTotalSummary(r.Context(), query)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}

		respondJson(rw, http.StatusOK, usage)
	}
}

func (h *apiHandler) queryRealtimeViewership() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		querySpec, httpErrorCode, errs := h.resolveRealtimeViewershipQuerySpec(r)
		if len(errs) > 0 {
			respondError(rw, httpErrorCode, errs...)
			return
		}
		metrics, err := h.views.QueryRealtimeEvents(r.Context(), querySpec)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) queryTimeSeriesRealtimeViewership() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		querySpec, httpErrorCode, errs := h.resolveTimeSeriesRealtimeViewershipQuerySpec(r)
		if len(errs) > 0 {
			respondError(rw, httpErrorCode, errs...)
			return
		}
		metrics, err := h.views.QueryTimeSeriesRealtimeEvents(r.Context(), querySpec)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}
		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) resolveViewershipQuerySpec(r *http.Request) (views.QuerySpec, int, []error) {
	var (
		from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
		to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
	)
	if errs := nonNilErrs(err1, err2); len(errs) > 0 {
		return views.QuerySpec{}, http.StatusBadRequest, errs
	}

	userId := callerUserId(r)
	if userId == "" {
		return views.QuerySpec{}, http.StatusInternalServerError, []error{errors.New("request not authenticated")}
	}
	projectId := callerProjectId(r)

	qs := r.URL.Query()
	assetID, streamID := qs.Get("assetId"), qs.Get("streamId")
	spec := views.QuerySpec{
		From:     from,
		To:       to,
		TimeStep: qs.Get("timeStep"),
		Filter: views.QueryFilter{
			UserID:     userId,
			ProjectID:  projectId,
			PlaybackID: qs.Get("playbackId"),
			CreatorID:  qs.Get("creatorId"),
		},
		BreakdownBy: qs["breakdownBy[]"],
	}
	spec, err := h.views.ResolvePlaybackId(spec, assetID, streamID)
	if err != nil {
		return views.QuerySpec{}, http.StatusInternalServerError, []error{err}
	}

	return spec, 0, []error{}
}

func (h *apiHandler) resolveRealtimeViewershipQuerySpec(r *http.Request) (views.QuerySpec, int, []error) {
	spec, httpErrorCode, errs := h.resolveViewershipQuerySpec(r)
	if spec.TimeStep != "" || spec.From != nil || spec.To != nil {
		return views.QuerySpec{}, http.StatusBadRequest, []error{errors.New("time range params (from, to, timeStep) are not supported for Realtime Viewership API")}
	}
	return spec, httpErrorCode, errs
}

func (h *apiHandler) resolveTimeSeriesRealtimeViewershipQuerySpec(r *http.Request) (views.QuerySpec, int, []error) {
	spec, httpErrorCode, errs := h.resolveViewershipQuerySpec(r)
	if spec.TimeStep != "" {
		return views.QuerySpec{}, http.StatusBadRequest, []error{errors.New("timeStep is not supported for Time Series Realtime Viewership API")}
	}
	if spec.From == nil {
		return views.QuerySpec{}, http.StatusBadRequest, []error{errors.New("param 'from' must be defined for Time Series Realtime Viewership API")}
	}
	// If using time range, then we allow to query max 1 min before now(),
	// because the current "per minute" aggregation may not be finalized yet
	lastToAllowed := time.Now().Add(-1 * time.Minute)
	if spec.To == nil || spec.To.After(lastToAllowed) {
		spec.To = &lastToAllowed
	}
	if spec.To.Sub(*spec.From) > 3*time.Hour {
		return views.QuerySpec{}, http.StatusBadRequest, []error{errors.New("requested time range cannot exceed 3 hours")}
	}
	return spec, httpErrorCode, errs
}

func (h *apiHandler) queryActiveUsersUsage() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {

		var (
			from, err1 = parseInputTimestamp(r.URL.Query().Get("from"))
			to, err2   = parseInputTimestamp(r.URL.Query().Get("to"))
		)

		if errs := nonNilErrs(err1, err2); len(errs) > 0 {
			respondError(rw, http.StatusBadRequest, errs...)
			return
		}

		if userId := callerUserId(r); userId == "" {
			respondError(rw, http.StatusInternalServerError, errors.New("request not authenticated"))
			return
		}

		if !isCallerAdmin(r) {
			respondError(rw, http.StatusForbidden, errors.New("only admins can query active users"))
			return
		}

		query := usage.FromToQuerySpec{
			From: from,
			To:   to,
		}

		usage, err := h.usage.QueryActiveUsageSummary(r.Context(), query)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}

		respondJson(rw, http.StatusOK, usage)
	}
}

func (h *apiHandler) getTotalViews(rw http.ResponseWriter, r *http.Request) {
	assetID := apiParam(r, assetIDParam)

	totalViews, err := h.views.Deprecated_GetTotalViews(r.Context(), assetID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	playbackID, oldStartViews := totalViews[0].ID, totalViews[0].StartViews
	if playbackID == "" {
		respondError(rw, http.StatusInternalServerError, errors.New("playbackId is empty"))
		return
	}

	metric, err := h.views.QuerySummary(r.Context(), playbackID)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	if metric != nil {
		totalViews = []prometheus.TotalViews{{
			ID:         totalViews[0].ID,
			StartViews: metric.ViewCount,
		}}
	}

	userId := callerUserId(r)
	glog.Infof("Used deprecated get total views endpoint userId=%v assetId=%v playbackId=%v oldStartViews=%v newViewCount=%v",
		userId, assetID, totalViews[0].ID, oldStartViews, totalViews[0].StartViews)

	respondJson(rw, http.StatusOK, totalViews)
}

func (h *apiHandler) queryRealtimeServerViewership() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		userId := r.URL.Query().Get("userId")
		if userId == "" {
			respondError(rw, http.StatusBadRequest, errors.New("userId is required"))
			return
		}

		if !isCallerAdmin(r) {
			respondError(rw, http.StatusForbidden, errors.New("only admins can query server-side viewership"))
			return
		}

		metrics, err := h.views.QueryRealtimeServerViews(r.Context(), userId)
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}

		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) queryAICapacity() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		metrics, err := h.ai.QueryAICapacity(r.Context(), r.URL.Query().Get("region"), r.URL.Query().Get("nodeId"))
		if err != nil {
			respondError(rw, http.StatusInternalServerError, err)
			return
		}

		respondJson(rw, http.StatusOK, metrics)
	}
}

func (h *apiHandler) getStreamHealth(rw http.ResponseWriter, r *http.Request) {
	respondJson(rw, http.StatusOK, getStreamStatus(r))
}

func (h *apiHandler) subscribeEvents(rw http.ResponseWriter, r *http.Request) {
	var (
		streamStatus = getStreamStatus(r)
		streamID     = streamStatus.ID
		sseOpts      = jsse.InitOptions(r).
				WithClientRetryBackoff(sseRetryBackoff).
				WithPing(ssePingDelay)

		lastEventID, err = parseInputUUID(sseOpts.LastEventID)
		from, err1       = parseInputTimestamp(r.URL.Query().Get("from"))
		to, err2         = parseInputTimestamp(r.URL.Query().Get("to"))
		mustFindLast, _  = strconv.ParseBool(r.URL.Query().Get("mustFindLast"))
	)
	if errs := nonNilErrs(err, err1, err2); len(errs) > 0 {
		respondError(rw, http.StatusBadRequest, errs...)
		return
	}

	var (
		pastEvents   []data.Event
		subscription <-chan data.Event
	)
	ctx, cancel := unionCtx(r.Context(), h.serverCtx)
	defer cancel()
	if to != nil {
		if from == nil {
			respondError(rw, http.StatusBadRequest, errors.New("query 'from' is required when using 'to'"))
			return
		}
		pastEvents, err = h.core.GetPastEvents(streamID, from, to)
	} else {
		pastEvents, subscription, err = h.core.SubscribeEvents(ctx, streamID, lastEventID, from)
		if err == health.ErrEventNotFound && !mustFindLast {
			pastEvents, subscription, err = h.core.SubscribeEvents(ctx, streamID, nil, nil)
		}
	}
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}

	sseEvents := makeSSEEventChan(ctx, pastEvents, subscription)
	err = jsse.ServeEvents(ctx, sseOpts, rw, sseEvents)
	if err != nil {
		status := http.StatusInternalServerError
		if httpErr, ok := err.(jsse.HTTPError); ok {
			status, err = httpErr.StatusCode, httpErr.Cause
		}
		glog.Errorf("Error serving events. err=%q", err)
		respondError(rw, status, err)
	}
}

func makeSSEEventChan(ctx context.Context, pastEvents []data.Event, subscription <-chan data.Event) <-chan jsse.Event {
	if subscription == nil {
		events := make(chan jsse.Event, len(pastEvents))
		for _, evt := range pastEvents {
			sendEvent(ctx, events, evt)
		}
		close(events)
		return events
	}
	events := make(chan jsse.Event, sseBufferSize)
	go func() {
		defer close(events)
		for _, evt := range pastEvents {
			if !sendEvent(ctx, events, evt) {
				return
			}
		}
		for evt := range subscription {
			if !sendEvent(ctx, events, evt) {
				return
			}
		}
	}()
	return events
}

func sendEvent(ctx context.Context, dest chan<- jsse.Event, evt data.Event) bool {
	sseEvt, err := toSSEEvent(evt)
	if err != nil {
		glog.Errorf("Skipping bad event due to error converting to SSE. evtID=%q, streamID=%q, err=%q", evt.ID(), evt.StreamID(), err)
		return true
	}
	select {
	case dest <- sseEvt:
		return true
	case <-ctx.Done():
		return false
	}
}
