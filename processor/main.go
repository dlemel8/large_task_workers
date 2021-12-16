package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	config "github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"protos"
)

var (
	processorDurations = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "task_sizes",
		Buckets: prometheus.ExponentialBuckets(1, 10, 8),
	})
)

type Processor struct {
	minDuration time.Duration
	maxDuration time.Duration
}

func (p Processor) Process(ctx context.Context, query *protos.ProcessQuery) (*protos.ProcessResult, error) {
	startTime := time.Now()

	var value uint8
	for _, x := range query.Data {
		value = (value + x) % 7
	}
	log.WithField("value", value).Debug("processed value")

	timeSoFar := time.Since(startTime)
	err := simulateCpuBoundWork(ctx, p.minDuration-timeSoFar, p.maxDuration-timeSoFar)

	processorDurations.Observe(float64(time.Since(startTime).Milliseconds()))
	return &protos.ProcessResult{Success: err == nil}, nil
}

func main() {
	log.Info("start to prepare services")
	rand.Seed(time.Now().UTC().UnixNano())
	config.AutomaticEnv()

	go servePrometheusMetrics(uint16(config.GetUint32("metrics_port")))

	server := &Processor{
		minDuration: config.GetDuration("external_processor_min_duration"),
		maxDuration: config.GetDuration("external_processor_max_duration"),
	}
	serveGrpc(uint16(config.GetUint32("grpc_port")), server)
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

func serveGrpc(port uint16, server protos.ProcessorServer) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	exitIfError(err, "failed to listen on grpc address")

	grpcServer := grpc.NewServer()
	protos.RegisterProcessorServer(grpcServer, server)
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	err = grpcServer.Serve(listener)
	exitIfError(err, "failed to serve grpc")
}

func simulateCpuBoundWork(ctx context.Context, minDuration time.Duration, maxDuration time.Duration) error {
	workDurationMs := minDuration.Milliseconds() + rand.Int63n(maxDuration.Milliseconds()-minDuration.Milliseconds())
	workDuration := time.Duration(workDurationMs) * time.Millisecond

	var err error
	startTime := time.Now()
	number := maxDuration.Milliseconds()
	for time.Since(startTime) < workDuration {
		err = ctx.Err()
		if err != nil {
			return err
		}

		number *= number
	}
	return nil
}
