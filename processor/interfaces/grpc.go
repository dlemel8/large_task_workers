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
	handler *application.TaskHandler
}

func (s *processorServer) ProcessInternalDataTask(ctx context.Context, query *protos.InternalDataQuery) (*protos.ProcessResult, error) {
	err := s.handler.HandleInternalTaskData(ctx, query.Labels, query.Data)
	return &protos.ProcessResult{Success: err == nil}, nil
}

func (s *processorServer) ProcessExternalDataTask(ctx context.Context, query *protos.ExternalDataQuery) (*protos.ProcessResult, error) {
	err := s.handler.HandleExternalTaskData(ctx, query.Labels, query.DataKey)

	return &protos.ProcessResult{Success: err == nil}, nil
}

func ServeGrpc(port uint16, handler *application.TaskHandler) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen on grpc address")
	}

	grpcServer := grpc.NewServer()
	protos.RegisterProcessorServer(grpcServer, &processorServer{handler: handler})
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	return grpcServer.Serve(listener)
}
