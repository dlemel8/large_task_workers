package application

import (
	"context"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Labels []string
	Data   []byte
}

type Reporter interface {
	ProcessedTask(duration time.Duration, success bool)
}

type TaskProcessor struct {
	minDuration time.Duration
	maxDuration time.Duration
	reporter    Reporter
}

func NewTaskProcessor(minDuration, maxDuration time.Duration, reporter Reporter) *TaskProcessor {
	return &TaskProcessor{
		minDuration: minDuration,
		maxDuration: maxDuration,
		reporter:    reporter,
	}
}

func (p *TaskProcessor) Process(ctx context.Context, task *Task) error {
	startTime := time.Now()

	moduloDivisor := uint8(len(task.Labels))
	var value uint8
	for _, x := range task.Data {
		value = (value + x) % moduloDivisor
	}
	log.WithField("value", value).Debug("processed value")

	timeSoFar := time.Since(startTime)
	err := simulateCpuBoundWork(ctx, p.minDuration-timeSoFar, p.maxDuration-timeSoFar)

	p.reporter.ProcessedTask(time.Since(startTime), err == nil)
	return err
}

func simulateCpuBoundWork(ctx context.Context, minDuration time.Duration, maxDuration time.Duration) error {
	minDurationMs := minDuration.Milliseconds()
	if minDurationMs <= 0 {
		minDurationMs = 0
	}

	maxDurationMS := maxDuration.Milliseconds()
	if maxDurationMS <= 0 || maxDurationMS-minDurationMs <= 0 {
		return nil
	}

	workDurationMs := minDurationMs + rand.Int63n(maxDurationMS-minDurationMs)
	workDuration := time.Duration(workDurationMs) * time.Millisecond

	var err error
	startTime := time.Now()
	number := maxDurationMS
	for time.Since(startTime) < workDuration {
		err = ctx.Err()
		if err != nil {
			return err
		}

		number *= number
	}
	return nil
}
