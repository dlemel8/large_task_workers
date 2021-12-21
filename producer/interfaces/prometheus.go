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
	generateTaskSizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "generated_task_sizes",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	}, []string{"success"})
	publishDurationsMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "publish_durations_ms",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	}, []string{"success"})
)

type Reporter struct{}

func (r *Reporter) GenerateTaskData(dataSize int, success bool) {
	successStr := strconv.FormatBool(success)
	value := float64(dataSize)
	generateTaskSizes.WithLabelValues(successStr).Observe(value)
}

func (r *Reporter) PublishDuration(duration time.Duration, success bool) {
	successStr := strconv.FormatBool(success)
	value := float64(duration.Milliseconds())
	publishDurationsMs.WithLabelValues(successStr).Observe(value)
}

func ServePrometheusMetrics(port uint16) error {
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
