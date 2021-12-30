package infrastructure

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
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

	if syscall.Access(directory, syscall.O_RDWR) != nil {
		return nil, fmt.Errorf("%s is not writable", directory)
	}

	return &FileStore{directory: directory}, nil
}

func (s *FileStore) Save(bytes []byte, key string) error {
	return os.WriteFile(s.filePath(key), bytes, 0777)
}

func (s *FileStore) Delete(key string) error {
	return os.Remove(s.filePath(key))
}

func (s *FileStore) filePath(key string) string {
	return filepath.Join(s.directory, key)
}
