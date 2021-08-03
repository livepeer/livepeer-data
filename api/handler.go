package api

import (
	"encoding/json"
	"net/http"
	"path"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/livepeer-data/health"
)

type Handler struct {
	http.Handler
	core *health.Core
}

func NewHandler(apiRoot string, healthcore *health.Core) http.Handler {
	router := httprouter.New()
	handler := &Handler{router, healthcore}

	{
		streamRoot := path.Join(apiRoot, "/stream/:manifestId")
		router.GET(streamRoot+"/health", handler.getStreamHealth)
	}
	return handler
}

func (h *Handler) getStreamHealth(rw http.ResponseWriter, r *http.Request, params httprouter.Params) {
	manifestID := params.ByName("manifestId")
	status, err := h.core.GetStatus(manifestID)
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
