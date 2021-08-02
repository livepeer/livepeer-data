package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
)

func NewHandler(healthcore *health.Core) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stream/health/", getStreamHealth(healthcore))
	return mux
}

func getStreamHealth(healthcore *health.Core) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 6)
		if len(parts) != 5 {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		manifestID := parts[4]
		status, err := healthcore.GetStatus(manifestID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == health.ErrStreamNotFound {
				status = http.StatusNotFound
			}
			rw.WriteHeader(status)
			return
		}
		rw.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(rw).Encode(status); err != nil {
			glog.Errorf("Error writing stream health JSON response. err=%q", err)
		}
	}
}
