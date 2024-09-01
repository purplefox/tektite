package objstore

import (
	"context"
	"time"
)

type Client interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	PutIfNotExists(ctx context.Context, key []byte, value []byte) (bool, error)
	Delete(ctx context.Context, key []byte) error
	DeleteAll(ctx context.Context, keys [][]byte) error
	ListObjectsWithPrefix(ctx context.Context, prefix []byte) ([]ObjectInfo, error)
	Start() error
	Stop() error
}

type ObjectInfo struct {
	Key          []byte
	LastModified time.Time
}
