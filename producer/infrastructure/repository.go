package infrastructure

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"producer/application"
	"protos"
)

type BytesPublisher interface {
	Publish(ctx context.Context, bytes []byte) error
}

type Repository struct {
	id        string
	publisher BytesPublisher
}

func NewRepository(id string, publisher BytesPublisher) *Repository {
	return &Repository{
		id:        id,
		publisher: publisher,
	}
}

func (r *Repository) SaveWholeTask(ctx context.Context, task *application.Task) error {
	metadataBytes, err := r.serializeTaskMetadata(task.Metadata)
	if err != nil {
		return err
	}

	taskBytes := append(task.Data, metadataBytes...)
	return r.publisher.Publish(ctx, taskBytes)
}

func (r *Repository) SaveTaskData(ctx context.Context, data []byte) (location string, err error) {
	//TODO implement me
	panic("implement me")
}

func (r *Repository) SaveTaskMetadata(ctx context.Context, metadata *application.TaskMetadata, dataLocation string) error {
	//TODO implement me
	panic("implement me")
}

type TaskPublisher struct {
	generator *application.TaskRandomizer
}

func (r *Repository) serializeTaskMetadata(metadata *application.TaskMetadata) ([]byte, error) {
	protoMetadata := &protos.Metadata{
		TaskId: metadata.Id,
		Generated: &protos.Generated{
			By: r.id,
			At: timestamppb.New(metadata.GeneratedAt),
		},
		Counters: metadata.Counters,
		Labels:   metadata.Labels,
	}

	return proto.Marshal(protoMetadata)
}
