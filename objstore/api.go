package objstore

import "time"

type Client interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	PutIfNotExists(key []byte, value []byte) (bool, error)
	Delete(key []byte) error
	DeleteAll(keys [][]byte) error
	ListObjectsWithPrefix(prefix []byte) ([]ObjectInfo, error)
	Start() error
	Stop() error
}

type ObjectInfo struct {
	Key []byte
	LastModified time.Time
}
