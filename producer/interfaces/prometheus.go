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
	randomizedDataSizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "randomized_data_sizes",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	}, []string{"success"})
	generatedTaskDurationsMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "generated_task_durations_ms",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	}, []string{"success"})
)

type Reporter struct{}

func (r *Reporter) RandomizedData(size int, success bool) {
	successStr := strconv.FormatBool(success)
	value := float64(size)
	randomizedDataSizes.WithLabelValues(successStr).Observe(value)
}

func (r *Reporter) GeneratedTask(duration time.Duration, success bool) {
	successStr := strconv.FormatBool(success)
	value := float64(duration.Milliseconds())
	generatedTaskDurationsMs.WithLabelValues(successStr).Observe(value)
}

func ServePrometheusMetrics(port uint16) error {
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
