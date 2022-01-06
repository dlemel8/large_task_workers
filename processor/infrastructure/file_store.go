package infrastructure

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"

	"processor/application"
)

type FileStore struct {
	directory string
}

func NewFileStore(directory string) (*FileStore, error) {
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

	return &FileStore{directory: directory}, nil
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

	file, err := os.OpenFile(filePath, os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	mmapped, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &mmapedData{
		data: mmapped,
		file: file,
	}, nil
}
