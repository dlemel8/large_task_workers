package main

import (
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	config "github.com/spf13/viper"

	"processor/application"
	"processor/interfaces"
)

func main() {
	log.Info("start to prepare services")
	rand.Seed(time.Now().UTC().UnixNano())
	config.AutomaticEnv()

	go func() {
		err := interfaces.ServePrometheusMetrics(uint16(config.GetUint32("metrics_port")))
		exitIfError(err, "failed to serve metrics")
	}()

	taskProcessor := application.NewTaskProcessor(
		config.GetDuration("external_processor_min_duration"),
		config.GetDuration("external_processor_max_duration"),
		new(interfaces.Reporter),
	)

	err := interfaces.ServeGrpc(uint16(config.GetUint32("grpc_port")), taskProcessor)
	exitIfError(err, "failed to serve grpc")
}

func exitIfError(err error, message string) {
	if err != nil {
		log.WithError(err).Fatal(message)
	}
}
