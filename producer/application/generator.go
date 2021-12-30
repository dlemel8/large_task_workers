package application

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

type TaskRepository interface {
	SaveWholeTask(ctx context.Context, task *Task) error
	SaveTaskData(ctx context.Context, data []byte) (location string, err error)
	SaveTaskMetadata(ctx context.Context, metadata *TaskMetadata, dataLocation string) error
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

	location, err := g.repository.SaveTaskData(ctx, task.Data)
	if err != nil {
		return err
	}

	return g.repository.SaveTaskMetadata(ctx, task.Metadata, location)
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
		g.reporter.GeneratedTask(time.Since(startTime), err == nil)
	}
}
