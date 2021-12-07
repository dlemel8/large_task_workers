package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
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

type task struct {
	metadata *protos.Metadata
	data     []byte
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	viper.AutomaticEnv()

	go servePrometheusMetrics(uint16(viper.GetUint32("metrics_port")))

	hostname, err := os.Hostname()
	exitIfError(err, "failed to get hostname")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tasks := make(chan *task)
	for i := 0; i < int(viper.GetUint32("number_of_workers")); i++ {
		go func(workerId uint32) {
			exitIfError(generateTasks(ctx, hostname, workerId, tasks), "failed generate tasks")
		}(uint32(i))
	}

	go func() {
		exitIfError(publishTasks(ctx, tasks), "failed to publish tasks")
	}()

	<-ctx.Done()
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

func generateTasks(ctx context.Context, producerId string, workerId uint32, tasks chan<- *task) error {
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

		metadata := &protos.Metadata{
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

		tasks <- &task{
			metadata: metadata,
			data:     data,
		}
	}
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

func publishTasks(ctx context.Context, tasks <-chan *task) error {
	options, err := redis.ParseURL(viper.GetString("redis_url"))
	if err != nil {
		return errors.Wrap(err, "failed to connect to Redis")
	}

	client := redis.NewClient(options)
	queueName := viper.GetString("published_tasks_queue_name")
	queueMaxSize := viper.GetInt64("published_tasks_queue_max_size")
	for {
		select {
		case <-ctx.Done():
			break

		case task := <-tasks:
			metadataBytes, err := proto.Marshal(task.metadata)
			if err != nil {
				return errors.Wrap(err, "failed to marshal task metadata")
			}

			finalData := append(task.data, metadataBytes...)
			taskSizes.Observe(float64(len(finalData)))

			if err := client.RPush(ctx, queueName, finalData).Err(); err != nil {
				return errors.Wrap(err, "failed to push to tasks queue")
			}

			if err := client.LTrim(ctx, queueName, 0, queueMaxSize).Err(); err != nil {
				return errors.Wrap(err, "failed to trim tasks queue")
			}
		}
	}
}
