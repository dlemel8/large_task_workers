package infrastructure

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"producer/application"
	"protos"
)

type BytesPublisher interface {
	Publish(ctx context.Context, bytes []byte) error
}

type BytesStore interface {
	Save(bytes []byte, key string) error
	Delete(key string) error
}

type Repository struct {
	id        string
	publisher BytesPublisher
	store     BytesStore
}

func NewRepository(id string, publisher BytesPublisher, store BytesStore) *Repository {
	return &Repository{
		id:        id,
		publisher: publisher,
		store:     store,
	}
}

func (r *Repository) SaveWholeTask(ctx context.Context, task *application.Task) error {
	metadataBytes, err := r.serializeTaskMetadata(task.Metadata, nil)
	if err != nil {
		return err
	}

	taskBytes := append(task.Data, metadataBytes...)
	return r.publisher.Publish(ctx, taskBytes)
}

func (r *Repository) SaveTaskData(_ context.Context, taskId uint64, data []byte) (key string, err error) {
	key = fmt.Sprintf("%s_%d", r.id, taskId)
	err = r.store.Save(data, key)
	return
}

func (r *Repository) SaveTaskMetadata(ctx context.Context, metadata *application.TaskMetadata, dataKey string) error {
	metadataBytes, err := r.serializeTaskMetadata(metadata, &dataKey)
	if err != nil {
		return err
	}
	return r.publisher.Publish(ctx, metadataBytes)
}

func (r *Repository) DeleteTaskData(_ context.Context, dataKey string) error {
	return r.store.Delete(dataKey)
}

type TaskPublisher struct {
	generator *application.TaskRandomizer
}

func (r *Repository) serializeTaskMetadata(metadata *application.TaskMetadata, dataKey *string) ([]byte, error) {
	protoMetadata := &protos.Metadata{
		TaskId: metadata.Id,
		Generated: &protos.Generated{
			By: r.id,
			At: timestamppb.New(metadata.GeneratedAt),
		},
		Counters: metadata.Counters,
		Labels:   metadata.Labels,
	}

	if dataKey != nil {
		protoMetadata.DataKey = dataKey
	}

	return proto.Marshal(protoMetadata)
}
