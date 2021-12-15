package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Namespace = "livepeer"
	Subsystem = "analyzer"
	Factory   promauto.Factory

	inited bool
)

func Init() {
	if inited {
		panic("can only init monitor once")
	}
	inited = true
	Factory = promauto.With(prometheus.DefaultRegisterer)
}

func FQName(name string) string {
	return prometheus.BuildFQName(Namespace, Subsystem, name)
}
