package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const sizeHeaderSizeInBytes = 4

type taskMetadata struct {
	producerId  string
	workerId    uint32
	taskId      uint64
	generatedAt time.Time
}

var (
	taskSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "task_sizes",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	})
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	viper.AutomaticEnv()

	go servePrometheusMetrics(uint16(viper.GetUint32("metrics_port")))

	hostname, err := os.Hostname()
	exitIfError(err, "failed to get hostname")

	err = generateTasks(hostname)
	exitIfError(err, "failed generate tasks")
}

func generateTasks(producerId string) error {
	largeTaskPercentage := viper.GetInt("large_task_percentage")
	largeTaskMinSize := int(viper.GetSizeInBytes("large_task_min_size"))
	largeTaskMaxSize := int(viper.GetSizeInBytes("large_task_max_size"))
	smallTaskMinSize := int(viper.GetSizeInBytes("small_task_min_size"))
	smallTaskMaxSize := int(viper.GetSizeInBytes("small_task_max_size"))

	for {
		metadata := taskMetadata{
			producerId:  producerId,
			workerId:    0,
			taskId:      rand.Uint64(),
			generatedAt: time.Now(),
		}

		isTaskLarge := rand.Intn(100) < largeTaskPercentage

		var data []byte
		var err error
		if isTaskLarge {
			data, err = getTaskDataWithSizeHeader(largeTaskMinSize, largeTaskMaxSize)
		} else {
			data, err = getTaskDataWithSizeHeader(smallTaskMinSize, smallTaskMaxSize)
		}
		if err != nil {
			return errors.Wrap(err, "failed to create task data")
		}

		metadataBytes, err := json.Marshal(metadata)
		if err != nil {
			return errors.Wrap(err, "failed to create task data")
		}

		finalData := append(data, metadataBytes...)
		taskSizes.Observe(float64(len(finalData)))
	}
}

func exitIfError(err error, message string) {
	if err != nil {
		log.WithError(err).Fatal(message)
	}
}

func servePrometheusMetrics(port uint16) {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	exitIfError(err, "failed to serve metrics")
}

func getTaskDataWithSizeHeader(minDataSize int, maxDataSize int) ([]byte, error) {
	dataSize := minDataSize + rand.Intn(maxDataSize-minDataSize)
	data := make([]byte, sizeHeaderSizeInBytes+dataSize)

	if _, err := rand.Read(data[sizeHeaderSizeInBytes:cap(data)]); err != nil {
		return nil, errors.Wrap(err, "failed to generate task data")
	}

	binary.BigEndian.PutUint32(data[:sizeHeaderSizeInBytes], uint32(dataSize))
	return data, nil
}
