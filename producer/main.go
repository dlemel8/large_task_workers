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

type MessagingStrategy string

const (
	metadataAndDataInRedis          MessagingStrategy = "MetadataAndDataInRedis"
	metadataAndDataInRabbitMq                         = "MetadataAndDataInRabbitMq"
	metadataAndDataInNng                              = "MetadataAndDataInNng"
	metadataInRabbitMqAndDataInFile                   = "MetadataInRabbitMqAndDataInFile"
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

	strategy := MessagingStrategy(config.GetString("messaging_strategy"))
	bytesPublisher, err := initializeBytesPublisher(strategy)
	exitIfError(err, "failed to initialize bytes publisher")

	fileStore, err := infrastructure.NewFileStore(config.GetString("file_store_path"))
	exitIfError(err, "failed to initialize file store")

	reporter := new(interfaces.Reporter)
	generator := application.NewTasksGenerator(
		application.NewTaskGenerator(
			application.NewTaskRandomize(
				int(config.GetUint32("large_task_percentage")),
				application.NewDataRandomize(
					int(config.GetSizeInBytes("large_task_min_size")),
					int(config.GetSizeInBytes("large_task_max_size")),
					reporter,
				),
				application.NewDataRandomize(
					int(config.GetSizeInBytes("small_task_min_size")),
					int(config.GetSizeInBytes("small_task_max_size")),
					reporter,
				),
			),
			infrastructure.NewRepository(hostname, bytesPublisher, fileStore),
		),
		reporter,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	generator.Generate(ctx, strategy != metadataInRabbitMqAndDataInFile)
	log.Info("goodbye")
}

func initializeBytesPublisher(strategy MessagingStrategy) (infrastructure.BytesPublisher, error) {
	publishedTasksQueueName := config.GetString("published_tasks_queue_name")
	publishedTasksQueueMaxSize := config.GetInt64("published_tasks_queue_max_size")
	switch strategy {
	case metadataAndDataInRedis:
		return infrastructure.NewRedisPublisher(
			config.GetString("redis_url"),
			publishedTasksQueueName,
			publishedTasksQueueMaxSize,
		)
	case metadataAndDataInRabbitMq:
		fallthrough
	case metadataInRabbitMqAndDataInFile:
		return infrastructure.NewRabbitMqPublisher(
			config.GetString("rabbitmq_url"),
			publishedTasksQueueName,
			publishedTasksQueueMaxSize,
		)
	case metadataAndDataInNng:
		return infrastructure.NewNanoMsgPublisher(
			config.GetString("nng_url"),
			publishedTasksQueueMaxSize,
		)

	default:
		return nil, fmt.Errorf("unsupported messaging strategy %s", strategy)
	}
}

func exitIfError(err error, message string) {
	if err != nil {
		log.WithError(err).Fatal(message)
	}
}
