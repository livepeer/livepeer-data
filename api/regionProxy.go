package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health/reducers"
	"github.com/nbio/hitch"
)

func regionProxy(hostFormat, ownRegion string) hitch.Middleware {
	proxy := &httputil.ReverseProxy{
		Director:      regionProxyDirector(hostFormat),
		FlushInterval: 100 * time.Millisecond,
	}
	return inlineMiddleware(func(rw http.ResponseWriter, r *http.Request, next http.Handler) {
		status := getStreamStatus(r)
		streamRegion := reducers.GetLastActiveData(status).Region
		if ownRegion == "" || streamRegion == "" || streamRegion == ownRegion {
			next.ServeHTTP(rw, r)
			return
		}
		if _, ok := r.Header[proxyLoopHeader]; ok {
			respondError(rw, http.StatusLoopDetected, errors.New("proxy loop detected"))
			return
		}
		proxy.ServeHTTP(rw, r)
	})
}

func regionProxyDirector(hostFormat string) func(req *http.Request) {
	return func(req *http.Request) {
		glog.V(8).Infof("Proxying request url=%s headers=%+v", req.URL, req.Header)
		status := getStreamStatus(req)
		streamRegion := reducers.GetLastActiveData(status).Region

		req.URL.Scheme = "http"
		if fwdProto := req.Header.Get("X-Forwarded-Proto"); fwdProto != "" {
			req.URL.Scheme = fwdProto
		}
		req.URL.Host = fmt.Sprintf(hostFormat, streamRegion)
		req.Host = req.URL.Host

		req.Header.Set(proxyLoopHeader, "analyzer")
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}
}
