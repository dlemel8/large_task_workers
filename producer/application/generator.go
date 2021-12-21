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

type Task struct {
	Id          uint64
	GeneratedAt time.Time
	Counters    map[string]uint32
	Labels      []string
	Data        []byte
}

type TaskGenerator struct {
	largeTaskPercentage int
	largeDataGenerator  *DataGenerator
	smallDataGenerator  *DataGenerator
}

func NewTaskGenerator(largeTaskPercentage int, largeDataGenerator, smallDataGenerator *DataGenerator) *TaskGenerator {
	return &TaskGenerator{
		largeTaskPercentage: largeTaskPercentage,
		largeDataGenerator:  largeDataGenerator,
		smallDataGenerator:  smallDataGenerator,
	}
}

func (g *TaskGenerator) Generate() (*Task, error) {
	isTaskLarge := rand.Intn(100) < g.largeTaskPercentage

	var data []byte
	var err error
	if isTaskLarge {
		data, err = g.largeDataGenerator.generate()
	} else {
		data, err = g.largeDataGenerator.generate()
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create task data")
	}

	return &Task{
		Id:          rand.Uint64(),
		GeneratedAt: time.Now(),
		Counters:    generateCounters(),
		Labels:      generateLabels(),
		Data:        data,
	}, nil
}

func generateCounters() map[string]uint32 {
	result := make(map[string]uint32)
	for _, counter := range counters {
		result[counter] = rand.Uint32()
	}
	return result
}

func generateLabels() []string {
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

type Reporter interface {
	GenerateTaskData(dataSize int, success bool)
}

type DataGenerator struct {
	minSizeInBytes int
	maxSizeInBytes int
	reporter       Reporter
}

func NewDataGenerator(minSizeInBytes, maxSizeInBytes int, reporter Reporter) *DataGenerator {
	return &DataGenerator{minSizeInBytes: minSizeInBytes, maxSizeInBytes: maxSizeInBytes, reporter: reporter}
}

func (g *DataGenerator) generate() ([]byte, error) {
	dataSize := g.minSizeInBytes + rand.Intn(g.maxSizeInBytes-g.minSizeInBytes)
	data := make([]byte, sizeHeaderSizeInBytes+dataSize)

	if _, err := rand.Read(data[sizeHeaderSizeInBytes:cap(data)]); err != nil {
		g.reporter.GenerateTaskData(dataSize, false)
		return nil, errors.Wrap(err, "failed to generate task data")
	}

	g.reporter.GenerateTaskData(dataSize, true)
	binary.BigEndian.PutUint32(data[:sizeHeaderSizeInBytes], uint32(dataSize))
	return data, nil
}
