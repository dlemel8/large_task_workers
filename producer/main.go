package main

import (
	"context"
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
	config "github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

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
	log.Info("start to prepare workers")
	rand.Seed(time.Now().UTC().UnixNano())
	config.AutomaticEnv()

	go servePrometheusMetrics(uint16(config.GetUint32("metrics_port")))

	hostname, err := os.Hostname()
	exitIfError(err, "failed to get hostname")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	numberOfWorkers := int(config.GetUint32("number_of_workers"))
	tasks := make(chan *task, numberOfWorkers)
	for i := 0; i < numberOfWorkers; i++ {
		go func(workerId uint32) {
			exitIfError(generateTasks(ctx, hostname, workerId, tasks), "failed generate tasks")
		}(uint32(i))
	}

	go func() {
		exitIfError(publishTasks(ctx, tasks), "failed to publish tasks")
	}()

	log.Info("everything is set")
	<-ctx.Done()
	log.Info("goodbye")
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
	log.Info("start to serve metrics")
}

func generateTasks(ctx context.Context, producerId string, workerId uint32, tasks chan<- *task) error {
	largeTaskPercentage := int(config.GetUint32("large_task_percentage"))
	largeTaskMinSize := int(config.GetSizeInBytes("large_task_min_size"))
	largeTaskMaxSize := int(config.GetSizeInBytes("large_task_max_size"))
	smallTaskMinSize := int(config.GetSizeInBytes("small_task_min_size"))
	smallTaskMaxSize := int(config.GetSizeInBytes("small_task_max_size"))

	log.WithField("workerId", workerId).Info("start to generate tasks")
	for {
		if ctx.Err() != nil {
			log.WithField("workerId", workerId).Info("generate tasks canceled")
			return nil
		}

		metadata := generateMetadata(producerId)

		isTaskLarge := rand.Intn(100) < largeTaskPercentage

		var data []byte
		var err error
		if isTaskLarge {
			data, err = generateTaskDataWithSizeHeader(largeTaskMinSize, largeTaskMaxSize)
		} else {
			data, err = generateTaskDataWithSizeHeader(smallTaskMinSize, smallTaskMaxSize)
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

func publishTasks(ctx context.Context, tasks <-chan *task) error {
	options, err := redis.ParseURL(config.GetString("redis_url"))
	if err != nil {
		return errors.Wrap(err, "failed to connect to Redis")
	}

	client := redis.NewClient(options)
	queueName := config.GetString("published_tasks_queue_name")
	queueMaxSize := config.GetInt64("published_tasks_queue_max_size")

	log.Info("start to publish tasks")
	for {
		select {
		case <-ctx.Done():
			log.Info("publish tasks cancelled")
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
