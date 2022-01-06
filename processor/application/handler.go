package application

import (
	"context"
	"io"

	log "github.com/sirupsen/logrus"
)

type ClosableData interface {
	io.Closer
	Data() []byte
}

type BytesStore interface {
	Load(key string) (ClosableData, error)
}

type TaskHandler struct {
	processor *TaskProcessor
	store     BytesStore
}

func NewTaskHandler(processor *TaskProcessor, store BytesStore) *TaskHandler {
	return &TaskHandler{
		processor: processor,
		store:     store,
	}
}

func (h *TaskHandler) HandleInternalTaskData(ctx context.Context, labels []string, data []byte) error {
	return h.processor.Process(ctx, &Task{
		Labels: labels,
		Data:   data,
	})
}

func (h *TaskHandler) HandleExternalTaskData(ctx context.Context, labels []string, dataKey string) error {
	closableData, err := h.store.Load(dataKey)
	if err != nil {
		return err
	}

	defer func() {
		if err = closableData.Close(); err != nil {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"key":        dataKey,
			}).WithError(err).Error("failed to close key data")
		}
	}()

	return h.processor.Process(ctx, &Task{
		Labels: labels,
		Data:   closableData.Data(),
	})
}
