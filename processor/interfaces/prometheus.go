package interfaces

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	processorDurations = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "processor_durations",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	}, []string{"success"})
)

type Reporter struct{}

func (r *Reporter) ProcessedTask(duration time.Duration, success bool) {
	successStr := strconv.FormatBool(success)
	value := float64(duration.Milliseconds())
	processorDurations.WithLabelValues(successStr).Observe(value)
}

func ServePrometheusMetrics(port uint16) error {
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
