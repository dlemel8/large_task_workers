package main

import (
	"context"
	cryptoRand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/timestamppb"

	"protos"
)

const sizeHeaderSizeInBytes = 4

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

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	for i := 0; i < int(viper.GetUint32("number_of_workers")); i++ {
		go func(workerId uint32) {
			err := generateTasks(ctx, hostname, workerId)
			exitIfError(err, "failed generate tasks")
		}(uint32(i))
	}

	<-ctx.Done()
}

func generateTasks(ctx context.Context, producerId string, workerId uint32) error {
	largeTaskPercentage := int(viper.GetUint32("large_task_percentage"))
	largeTaskMinSize := int(viper.GetSizeInBytes("large_task_min_size"))
	largeTaskMaxSize := int(viper.GetSizeInBytes("large_task_max_size"))
	smallTaskMinSize := int(viper.GetSizeInBytes("small_task_min_size"))
	smallTaskMaxSize := int(viper.GetSizeInBytes("small_task_max_size"))

	for {
		if ctx.Err() != nil {
			log.Info("canceled")
			return nil
		}

		metadata := protos.Metadata{
			ProducerId:  producerId,
			WorkerId:    workerId,
			TaskId:      rand.Uint64(),
			GeneratedAt: timestamppb.Now(),
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
			return errors.Wrap(err, "failed to encode task metadata")
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

	if _, err := cryptoRand.Read(data[sizeHeaderSizeInBytes:cap(data)]); err != nil {
		return nil, errors.Wrap(err, "failed to generate task data")
	}

	binary.BigEndian.PutUint32(data[:sizeHeaderSizeInBytes], uint32(dataSize))
	return data, nil
}
