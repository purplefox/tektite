package dev

import (
	"bytes"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func NewInMemStore(delay time.Duration) *InMemStore {
	return &InMemStore{delay: delay}
}

var _ objstore.Client = &InMemStore{}

// InMemStore - LocalStore is a simple cloud store used for testing
type InMemStore struct {
	store       sync.Map
	delay       time.Duration
	unavailable atomic.Bool
	condLock    sync.Mutex
}

type valueHolder struct {
	value []byte
	info objstore.ObjectInfo
}

func (im *InMemStore) PutIfNotExists(key []byte, value []byte) (bool, error) {
	if err := im.checkUnavailable(); err != nil {
		return false, err
	}
	im.maybeAddDelay()
	im.condLock.Lock()
	defer im.condLock.Unlock()
	skey := common.ByteSliceToStringZeroCopy(key)
	_, exists := im.store.Load(skey)
	if exists {
		return false, nil
	}
	im.store.Store(skey, valueHolder{value: value, info: objstore.ObjectInfo{Key: key, LastModified: time.Now()}})
	return true, nil
}

func (im *InMemStore) ListObjectsWithPrefix(prefix []byte) ([]objstore.ObjectInfo, error) {
	sPref := string(prefix)
	var infos []objstore.ObjectInfo
	im.store.Range(func(k, v interface{}) bool {
		key := k.(string)
		if len(prefix) == 0 || strings.HasPrefix(key, sPref) {
			infos = append(infos, objstore.ObjectInfo{
				Key: []byte(key),
				LastModified: v.(valueHolder).info.LastModified,
			})
		}
		return true
	})
	sort.SliceStable(infos, func(i, j int) bool {
		return bytes.Compare(infos[i].Key, infos[j].Key) < 0
	})
	return infos, nil
}

func (im *InMemStore) Get(key []byte) ([]byte, error) {
	if err := im.checkUnavailable(); err != nil {
		return nil, err
	}
	im.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	v, ok := im.store.Load(skey)
	if !ok {
		return nil, nil
	}
	if v == nil {
		panic("nil value in obj store")
	}
	holder := v.(valueHolder)
	return holder.value, nil //nolint:forcetypeassert
}

func (im *InMemStore) Put(key []byte, value []byte) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	im.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	log.Debugf("local cloud store %p adding blob with key %v value length %d", im, key, len(value))
	im.store.Store(skey, valueHolder{value: value, info: objstore.ObjectInfo{Key: key, LastModified: time.Now()}})
	return nil
}

func (im *InMemStore) Delete(key []byte) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	log.Debugf("local cloud store %p deleting obj with key %v", im, key)
	im.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	im.store.Delete(skey)
	return nil
}

func (im *InMemStore) DeleteAll(keys [][]byte) error {
	if err := im.checkUnavailable(); err != nil {
		return err
	}
	im.maybeAddDelay()
	for _, key := range keys {
		im.store.Delete(string(key))
	}
	return nil
}

func (im *InMemStore) SetUnavailable(unavailable bool) {
	im.unavailable.Store(unavailable)
}

func (im *InMemStore) checkUnavailable() error {
	if im.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "cloud store is unavailable")
	}
	return nil
}

func (im *InMemStore) maybeAddDelay() {
	if im.delay != 0 {
		time.Sleep(im.delay)
	}
}

func (im *InMemStore) Size() int {
	size := 0
	im.store.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func (im *InMemStore) ForEach(fun func(key string, value []byte)) {
	im.store.Range(func(k, v any) bool {
		fun(k.(string), v.([]byte))
		return true
	})
}

func (im *InMemStore) Start() error {
	return nil
}

func (im *InMemStore) Stop() error {
	return nil
}
