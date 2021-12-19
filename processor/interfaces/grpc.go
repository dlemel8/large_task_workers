package interfaces

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"processor/application"
	"protos"
)

type processorServer struct {
	service *application.TaskProcessor
}

func (p *processorServer) Process(ctx context.Context, query *protos.ProcessQuery) (*protos.ProcessResult, error) {
	err := p.service.Process(ctx, &application.Task{
		Labels: query.Metadata.Labels,
		Data:   query.Data,
	})

	return &protos.ProcessResult{Success: err == nil}, nil
}

func ServeGrpc(port uint16, service *application.TaskProcessor) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen on grpc address")
	}

	grpcServer := grpc.NewServer()
	protos.RegisterProcessorServer(grpcServer, &processorServer{service: service})
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	return grpcServer.Serve(listener)
}
