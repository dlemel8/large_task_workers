package application

import (
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/pkg/errors"
)

const (
	maxLabelsPerMetadata  = 3
	sizeHeaderSizeInBytes = 4
)

var (
	counters = []string{"counter1", "counter2", "counter3", "counter4"}
	labels   = []string{"label1", "label2", "label3", "label4", "label5"}
)

type TaskMetadata struct {
	Id          uint64
	GeneratedAt time.Time
	Counters    map[string]uint32
	Labels      []string
}

type Task struct {
	Metadata *TaskMetadata
	Data     []byte
}

type TaskRandomizer struct {
	largeTaskPercentage int
	largeDataRandomizer *DataRandomizer
	smallDataRandomizer *DataRandomizer
}

func NewTaskRandomize(largeTaskPercentage int, largeDataRandomizer, smallDataRandomizer *DataRandomizer) *TaskRandomizer {
	return &TaskRandomizer{
		largeTaskPercentage: largeTaskPercentage,
		largeDataRandomizer: largeDataRandomizer,
		smallDataRandomizer: smallDataRandomizer,
	}
}

func (g *TaskRandomizer) Randomize() (*Task, error) {
	isTaskLarge := rand.Intn(100) < g.largeTaskPercentage

	var data []byte
	var err error
	if isTaskLarge {
		data, err = g.largeDataRandomizer.Randomize()
	} else {
		data, err = g.largeDataRandomizer.Randomize()
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create task data")
	}

	return &Task{
		Metadata: &TaskMetadata{
			Id:          rand.Uint64(),
			GeneratedAt: time.Now(),
			Counters:    randomizeCounters(),
			Labels:      randomizeLabels(),
		},
		Data: data,
	}, nil
}

func randomizeCounters() map[string]uint32 {
	result := make(map[string]uint32)
	for _, counter := range counters {
		result[counter] = rand.Uint32()
	}
	return result
}

func randomizeLabels() []string {
	selected := make(map[string]interface{})
	for i := 0; i < maxLabelsPerMetadata; i++ {
		label := labels[rand.Intn(len(labels))]
		selected[label] = nil
	}

	result := make([]string, 0, len(selected))
	for label := range selected {
		result = append(result, label)
	}
	return result
}

type RandomizedDataReporter interface {
	RandomizedData(dataSize int, success bool)
}

type DataRandomizer struct {
	minSizeInBytes int
	maxSizeInBytes int
	reporter       RandomizedDataReporter
}

func NewDataRandomize(minSizeInBytes, maxSizeInBytes int, reporter RandomizedDataReporter) *DataRandomizer {
	return &DataRandomizer{minSizeInBytes: minSizeInBytes, maxSizeInBytes: maxSizeInBytes, reporter: reporter}
}

func (g *DataRandomizer) Randomize() ([]byte, error) {
	dataSize := g.minSizeInBytes + rand.Intn(g.maxSizeInBytes-g.minSizeInBytes)
	data := make([]byte, sizeHeaderSizeInBytes+dataSize)

	if _, err := rand.Read(data[sizeHeaderSizeInBytes:cap(data)]); err != nil {
		g.reporter.RandomizedData(dataSize, false)
		return nil, errors.Wrap(err, "failed to randomize task data")
	}

	g.reporter.RandomizedData(dataSize, true)
	binary.BigEndian.PutUint32(data[:sizeHeaderSizeInBytes], uint32(dataSize))
	return data, nil
}
