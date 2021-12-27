package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	config "github.com/spf13/viper"

	"producer/application"
	"producer/infrastructure"
	"producer/interfaces"
)

const (
	strategyMetadataAndDataInRedis    = "MetadataAndDataInRedis"
	strategyMetadataAndDataInRabbitMq = "MetadataAndDataInRabbitMQ"
)

func main() {
	log.Info("start to prepare workers")
	rand.Seed(time.Now().UTC().UnixNano())
	config.AutomaticEnv()

	go func() {
		err := interfaces.ServePrometheusMetrics(uint16(config.GetUint32("metrics_port")))
		exitIfError(err, "failed to serve metrics")
	}()

	hostname, err := os.Hostname()
	exitIfError(err, "failed to get hostname")

	reporter := new(interfaces.Reporter)
	generator := application.NewTaskGenerator(
		int(config.GetUint32("large_task_percentage")),
		application.NewDataGenerator(
			int(config.GetSizeInBytes("large_task_min_size")),
			int(config.GetSizeInBytes("large_task_max_size")),
			reporter,
		),
		application.NewDataGenerator(
			int(config.GetSizeInBytes("small_task_min_size")),
			int(config.GetSizeInBytes("small_task_max_size")),
			reporter,
		),
	)

	bytesPublisher, err := initializeBytesPublisher()
	exitIfError(err, "failed to initialize bytes publisher")

	tasksPublisher := infrastructure.NewTaskPublisher(hostname, generator, bytesPublisher, reporter)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	err = tasksPublisher.Publish(ctx)
	exitIfError(err, "failed to publish tasks")

	log.Info("goodbye")
}

func initializeBytesPublisher() (infrastructure.BytesPublisher, error) {
	publishedTasksQueueName := config.GetString("published_tasks_queue_name")
	publishedTasksQueueMaxSize := config.GetInt64("published_tasks_queue_max_size")
	strategy := config.GetString("strategy")
	switch strategy {
	case strategyMetadataAndDataInRedis:
		return infrastructure.NewRedisPublisher(
			config.GetString("redis_url"),
			publishedTasksQueueName,
			publishedTasksQueueMaxSize,
		)
	case strategyMetadataAndDataInRabbitMq:
		return infrastructure.NewRabbitMqPublisher(
			config.GetString("rabbitmq_url"),
			publishedTasksQueueName,
			publishedTasksQueueMaxSize,
		)
	default:
		return nil, fmt.Errorf("invalid strategy %s", strategy)
	}
}

func exitIfError(err error, message string) {
	if err != nil {
		log.WithError(err).Fatal(message)
	}
}
