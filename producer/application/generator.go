package application

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

type TaskRepository interface {
	SaveWholeTask(ctx context.Context, task *Task) error
	SaveTaskData(ctx context.Context, taskId uint64, data []byte) (key string, err error)
	SaveTaskMetadata(ctx context.Context, metadata *TaskMetadata, dataKey string) error
	DeleteTaskData(ctx context.Context, dataKey string) error
}

type TaskGenerator struct {
	randomizer *TaskRandomizer
	repository TaskRepository
}

func NewTaskGenerator(randomizer *TaskRandomizer, repository TaskRepository) *TaskGenerator {
	return &TaskGenerator{
		randomizer: randomizer,
		repository: repository,
	}
}

func (g *TaskGenerator) Generate(ctx context.Context, saveWholeTask bool) error {
	task, err := g.randomizer.Randomize()
	if err != nil {
		return err
	}

	if saveWholeTask {
		return g.repository.SaveWholeTask(ctx, task)
	}

	key, err := g.repository.SaveTaskData(ctx, task.Metadata.Id, task.Data)
	if err != nil {
		return err
	}

	err = g.repository.SaveTaskMetadata(ctx, task.Metadata, key)
	if err != nil {
		if deleteErr := g.repository.DeleteTaskData(ctx, key); err != nil {
			log.WithError(deleteErr).Error("failed to delete data after save metadata failure")
		}
	}
	return err
}

type GeneratedTaskReporter interface {
	GeneratedTask(duration time.Duration, success bool)
}

type TasksGenerator struct {
	generator *TaskGenerator
	reporter  GeneratedTaskReporter
}

func NewTasksGenerator(generator *TaskGenerator, reporter GeneratedTaskReporter) *TasksGenerator {
	return &TasksGenerator{
		generator: generator,
		reporter:  reporter,
	}
}

func (g *TasksGenerator) Generate(ctx context.Context, saveWholeTask bool) {
	log.Info("start to generate tasks")
	var err error
	for {
		err = ctx.Err()
		if err != nil {
			log.Info("publish generate cancelled")
			break
		}

		startTime := time.Now()
		err = g.generator.Generate(ctx, saveWholeTask)
		if err != nil {
			log.WithError(err).Error("failed to generate task")
		}
		g.reporter.GeneratedTask(time.Since(startTime), err == nil)
	}
}
