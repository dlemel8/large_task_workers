package infrastructure

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"producer/application"
	"protos"
)

type BytesPublisher interface {
	Publish(ctx context.Context, bytes []byte) error
}

type Reporter interface {
	PublishDuration(duration time.Duration, success bool)
}

type TaskPublisher struct {
	producerId string
	generator  *application.TaskGenerator
	publisher  BytesPublisher
	reporter   Reporter
}

func NewTaskPublisher(producerId string, generator *application.TaskGenerator, publisher BytesPublisher, reporter Reporter) *TaskPublisher {
	return &TaskPublisher{
		producerId: producerId,
		generator:  generator,
		publisher:  publisher,
		reporter:   reporter,
	}
}

func (p *TaskPublisher) Publish(ctx context.Context) error {
	log.Info("start to publish tasks")
	var err error
	for {
		err = ctx.Err()
		if err != nil {
			log.Info("publish tasks cancelled")
			break
		}

		p.publishNewTask(ctx)
	}

	return nil
}

func (p *TaskPublisher) publishNewTask(ctx context.Context) {
	startTime := time.Now()
	task, err := p.generator.Generate()
	if err != nil {
		p.reporter.PublishDuration(time.Since(startTime), false)
		return
	}

	taskBytes, err := p.serializeTask(task)
	if err != nil {
		p.reporter.PublishDuration(time.Since(startTime), false)
		return
	}

	err = p.publisher.Publish(ctx, taskBytes)
	if err != nil {
		p.reporter.PublishDuration(time.Since(startTime), false)
		return
	}

	p.reporter.PublishDuration(time.Since(startTime), true)
}

func (p *TaskPublisher) serializeTask(task *application.Task) ([]byte, error) {
	metadata := &protos.Metadata{
		TaskId: task.Id,
		Generated: &protos.Generated{
			By: p.producerId,
			At: timestamppb.New(task.GeneratedAt),
		},
		Counters: task.Counters,
		Labels:   task.Labels,
	}

	metadataBytes, err := proto.Marshal(metadata)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal task metadata")
	}

	finalData := append(task.Data, metadataBytes...)
	return finalData, err
}
