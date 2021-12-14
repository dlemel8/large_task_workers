package main

import (
	"encoding/binary"
	"math/rand"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"protos"
)

var (
	counters = []string{"counter1", "counter2", "counter3", "counter4"}
	labels   = []string{"label1", "label2", "label3", "label4", "label5"}
)

const maxLabelsPerMetadata = 3

func generateMetadata(producerId string) *protos.Metadata {
	return &protos.Metadata{
		TaskId: rand.Uint64(),
		Generated: &protos.Generated{
			By: producerId,
			At: timestamppb.Now(),
		},
		Counters: generateCounters(),
		Labels:   generateLabels(),
	}
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

func generateTaskDataWithSizeHeader(minDataSize int, maxDataSize int) ([]byte, error) {
	dataSize := minDataSize + rand.Intn(maxDataSize-minDataSize)
	data := make([]byte, sizeHeaderSizeInBytes+dataSize)

	if _, err := rand.Read(data[sizeHeaderSizeInBytes:cap(data)]); err != nil {
		return nil, errors.Wrap(err, "failed to generate task data")
	}

	binary.BigEndian.PutUint32(data[:sizeHeaderSizeInBytes], uint32(dataSize))
	return data, nil
}
