package infrastructure

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"

	"processor/application"
)

type FileLoadStrategy string

const (
	Syscall FileLoadStrategy = "Syscall"
	Mmap                     = "Mmap"
)

type FileStore struct {
	directory string
	strategy  FileLoadStrategy
}

func NewFileStore(directory string, strategy FileLoadStrategy) (*FileStore, error) {
	fileInfo, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to check directory %s information", directory)
	}

	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directory)
	}

	if syscall.Access(directory, syscall.O_RDONLY) != nil {
		return nil, fmt.Errorf("%s is not readable", directory)
	}

	return &FileStore{directory: directory, strategy: strategy}, nil
}

type loadedData struct {
	data []byte
}

func (d *loadedData) Close() error {
	return nil
}

func (d *loadedData) Data() []byte {
	return d.data
}

type mmapedData struct {
	data mmap.MMap
	file *os.File
}

func (d *mmapedData) Data() []byte {
	return d.data
}

func (d *mmapedData) Close() error {
	if err := d.data.Unmap(); err != nil {
		log.WithError(err).Error("failed to unmap mmaped file, it will be leaked!")
		return err
	}

	return d.file.Close()
}

func (s *FileStore) Load(key string) (application.ClosableData, error) {
	filePath := filepath.Join(s.directory, key)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	switch s.strategy {
	case Syscall:
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}

		return &loadedData{data: data}, nil

	case Mmap:
		mmapped, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			return nil, err
		}

		return &mmapedData{data: mmapped, file: file}, nil

	default:
		return nil, fmt.Errorf("unsupported strategy %s", s.strategy)
	}
}
