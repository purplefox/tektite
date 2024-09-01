package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

/*
StateMachine implements a distributed state machine which persists state via object storage. Clients can concurrently
update the state machine via their own instances of this struct.
It can be used as the base for many distributed concurrency primitives including group membership, distributed locks,
distributed sequences, etc.
The state maintained by the state machine is written as a JSON type to object storage. StateMachine state can be any
type that `json.Marshall` accepts a pointer to.
StateMachine utilises conditional writes via the `PutIfNotExists` method of the object store. This conditional and
atomically puts an object only if there is no existing object with the same key.
Each instance of StateMachine maintains an internal sequence. Every time it updates state, it tries to put a key
made up of a prefix and the sequence number. If the key is already there it means another StateMachine instance has
already updated state based on the previous state. In this case the latest state is loaded and the update tried again,
this is repeated until the update succeeds.
At startup, each instance list objects with prefix, and takes the key with the highest sequence as the starting state.
If an instance doesn't update for some time, and many updates have been made by other instances then it's sequence number
can lag far behind, and when it does update it will have many failed `PutIfNotExists` calls before it succeeds. To
prevent instances getting too far behind, no-op updates which do not change the state but unsure the sequence is up to date
or made periodically according to the `updateInterval` parameter.
Every time an update is made a new entry is created. Old entries need to be garbage collected (deleted) when they are
no longer needed. Determining whether an entry is needed is tricky. If a member has internal state with sequence number S
then we cannot delete keys with sequence > S, or state machine state will be lost. Also if a new member joins and
calls list objects to initialise it's sequence, then we cannot delete any sequence greater than the largest key listed.
We set an upper bound on the maximum time on how long we will retain the value of nextSequence in the state machine. If
we keep it too long, then it would be possible that the next key, which might already have been written gets deleted by
garbage collection. This would result in the next update succeeding but the consistency of the state machine would be
broken as in-use state would have been lost.
If we take too long since the current nextSequence was loaded we will timeout, and force a reload of latest key via `init()`.
Consequently we can ensure that no member relies on old sequence values, even in the presence of non availability of the
object store for an extended time.
Since we have a upper bound on how long keys are needed we make sure garbage collection never deletes keys more recent
than this time. In practice, we make the retention time much larger than the upper bound, to be on the safe side.
*/
type StateMachine[T any] struct {
	lock                     sync.Mutex
	address                  string
	keyPrefix                string
	objStoreClient           objstore.Client
	nextSequence             int
	sequenceDeadline         int64
	state                    T
	gcTimer                  *time.Timer
	gcInterval               time.Duration
	updateInterval           time.Duration
	retainedDuration         time.Duration
	unavailableRetryInterval time.Duration
	maxSequenceAge           time.Duration
	lastUpdateTime           int64
	updateTimer              *time.Timer
	started                  bool
	stopping                 atomic.Bool
}

const RetainedDurationFactor = 10
const MaxSequenceAgeFactor = 2

func NewStateMachine[T any](keyPrefix string, address string, objStoreClient objstore.Client, gcInterval time.Duration,
	updateInterval time.Duration) *StateMachine[T] {
	unavailableRetryInterval := updateInterval / 4
	retainedDuration := RetainedDurationFactor * updateInterval
	maxSequenceAge := MaxSequenceAgeFactor * updateInterval
	sm := &StateMachine[T]{
		address:                  address,
		keyPrefix:                keyPrefix,
		objStoreClient:           objStoreClient,
		nextSequence:             -1,
		gcInterval:               gcInterval,
		retainedDuration:         retainedDuration,
		updateInterval:           updateInterval,
		unavailableRetryInterval: unavailableRetryInterval,
		maxSequenceAge:           maxSequenceAge,
		lastUpdateTime:           -1,
	}
	return sm
}

func (s *StateMachine[T]) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	s.scheduleGC()
	s.scheduleUpdate()
	s.started = true
}

func (s *StateMachine[T]) Stop() {
	s.stopping.Store(true)
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	s.gcTimer.Stop()
	s.updateTimer.Stop()
	s.started = false
}

func (s *StateMachine[T]) scheduleGC() {
	// Each node will perform GC so we schedule with a random interval based on the configured interval
	delay := time.Duration(rand.Intn(int(s.gcInterval)) + (int(s.gcInterval) / 2))
	s.gcTimer = time.AfterFunc(delay, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.started {
			return
		}
		if err := s.garbageCollectKeys(); err != nil {
			log.Errorf("failed to garbage collect keys: %v", err)
		}
		s.scheduleGC()
	})
}

func (s *StateMachine[T]) scheduleUpdate() {
	s.updateTimer = time.AfterFunc(s.updateInterval, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.started {
			return
		}
		if int64(arista.NanoTime())-s.lastUpdateTime >= int64(s.updateInterval) {
			// If user hasn't updated, then we run a no-op update - this ensures our current sequence number remains
			// up to date with latest state.
			if _, err := s.doUpdate(func(state T) (T, error) {
				return state, nil
			}); err != nil {
				log.Errorf("failed to update: %v", err)
			}
		}
		s.scheduleUpdate()
	})
}

func (s *StateMachine[T]) garbageCollectKeys() error {
	existingInfos, err := s.listObjectsWithTimeout()
	if err != nil {
		return err
	}
	if len(existingInfos) == 0 {
		return nil
	}
	// Get last modified of newest key - we never delete the last retainedDuration of keys
	newest := existingInfos[len(existingInfos)-1].LastModified.UTC()
	deleteBefore := newest.Add(-s.retainedDuration)
	var toDelete [][]byte
	for _, info := range existingInfos {
		if info.LastModified.UTC().Before(deleteBefore) {
			toDelete = append(toDelete, info.Key)
		}
	}
	if len(toDelete) > 0 {
		if err := s.deleteObjectsWithTimeout(toDelete); err != nil {
			log.Errorf("Error deleting keys %v", err)
		}
	}
	return nil
}

func (s *StateMachine[T]) listObjectsWithTimeout() ([]objstore.ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.objStoreClient.ListObjectsWithPrefix(ctx, []byte(s.keyPrefix))
}

func (s *StateMachine[T]) deleteObjectsWithTimeout(toDelete [][]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.objStoreClient.DeleteAll(ctx, toDelete)
}

func (s *StateMachine[T]) updateSequenceDeadline() {
	s.sequenceDeadline = int64(arista.NanoTime()) + s.maxSequenceAge.Nanoseconds()
}

func (s *StateMachine[T]) init() error {
	for {
		s.updateSequenceDeadline()
		// Initialise next sequence to be 1 + largest stored sequence
		existingInfos, err := s.listKeysWithRetry()
		if err != nil {
			if errors.As(err, context.DeadlineExceeded) {
				// Took too long, retry
				continue
			}
			return err
		}
		if len(existingInfos) > 0 {
			lastInfo := existingInfos[len(existingInfos)-1]
			lastSeq, err := s.extractSequenceFromKey(lastInfo.Key)
			if err != nil {
				return err
			}
			s.nextSequence = lastSeq + 1

			buff, err := s.getWithRetry(lastInfo.Key)
			if err != nil {
				if errors.As(err, context.DeadlineExceeded) {
					// Took too long, retry
					continue
				}
				return err
			}
			if buff == nil {
				// Was deleted - continue
				continue
			}
			var state T
			if err := json.Unmarshal(buff, &state); err != nil {
				return err
			}
			s.state = state
		} else {
			s.nextSequence = 0
		}
		return nil
	}
}

func (s *StateMachine[T]) extractSequenceFromKey(key []byte) (int, error) {
	return strconv.Atoi(string(key[len(s.keyPrefix)+1:]))
}

// Update updates the state based on the previous state. The update function provides the operation to update the state
// based on the previous state, returning the new state which will be stored. The function returns when the new state
// has been committed to object storage, or an error occurs.
func (s *StateMachine[T]) Update(updateFunc func(state T) (T, error)) (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	state, err := s.doUpdate(updateFunc)
	s.lastUpdateTime = int64(arista.NanoTime())
	return state, err
}

func (s *StateMachine[T]) doUpdate(updateFunc func(state T) (T, error)) (T, error) {
	for {
		r, err := s.update(updateFunc)
		if err == nil {
			return r, nil
		}
		var zeroT T
		if errors.As(err, context.DeadlineExceeded) {
			// We took too long to do the update, we need to re-load sequence
			log.Infof("failed to update as object store unavailable - will reload")
			// the update failed as objects store was unavailable for > updateTime
			// To avoid possibility that an in use key was deleted, we must reload from latest key and update again
			if err := s.init(); err != nil {
				return zeroT, err
			}
			continue
		}
		return zeroT, err
	}
}

func (s *StateMachine[T]) update(updateFunc func(state T) (T, error)) (T, error) {
	var tZero T
	if s.nextSequence == -1 {
		if err := s.init(); err != nil {
			return tZero, err
		}
	}
	seq := s.nextSequence
	state := s.state
	for {
		var err error
		state, err = updateFunc(state)
		if err != nil {
			return tZero, err
		}
		buff, err := json.Marshal(&state)
		if err != nil {
			return tZero, err
		}
		newKey := s.createKey(seq)
		put, err := s.putIfNotExistsWithRetry([]byte(newKey), buff)
		if err != nil {
			return tZero, err
		}
		if put {
			s.nextSequence = seq + 1
			s.state = state
			s.updateSequenceDeadline()
			return s.state, nil
		}
		// key exists already - load the state
		buffRead, err := s.getWithRetry([]byte(newKey))
		if err != nil {
			return tZero, err
		}
		if buffRead == nil {
			return tZero, errors.Errorf("cannot find key %v", newKey)
		}
		var newState T
		if err := json.Unmarshal(buffRead, &newState); err != nil {
			return tZero, err
		}
		state = newState
		seq++
	}
}

func (s *StateMachine[T]) getWithRetry(key []byte) ([]byte, error) {
	v, err := s.executeWithRetry(func(ctx context.Context) (any, error) {
		return s.objStoreClient.Get(ctx, key)
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), err
}

func (s *StateMachine[T]) putIfNotExistsWithRetry(key []byte, value []byte) (bool, error) {
	v, err := s.executeWithRetry(func(ctx context.Context) (any, error) {
		return s.objStoreClient.PutIfNotExists(ctx, key, value)
	})
	if err != nil {
		return false, err
	}
	return v.(bool), err
}

func (s *StateMachine[T]) listKeysWithRetry() ([]objstore.ObjectInfo, error) {
	v, err := s.executeWithRetry(func(ctx context.Context) (any, error) {
		return s.objStoreClient.ListObjectsWithPrefix(ctx, []byte(s.keyPrefix))
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.([]objstore.ObjectInfo), err
}

func execActionWithCancel(action func(ctx context.Context) (any, error), timeout time.Duration) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return action(ctx)
}

func (s *StateMachine[T]) executeWithRetry(action func(ctx context.Context) (any, error)) (any, error) {
	for {
		if s.stopping.Load() {
			return nil, errors.New("state machine is stopping")
		}
		remaining := s.sequenceDeadline - int64(arista.NanoTime())
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		r, err := execActionWithCancel(action, time.Duration(remaining))
		if err == nil {
			return r, nil
		}
		if !common.IsUnavailableError(err) {
			return nil, err
		}
		remaining = s.sequenceDeadline - int64(arista.NanoTime())
		if remaining <= s.unavailableRetryInterval.Nanoseconds() {
			return nil, context.DeadlineExceeded
		}
		// retry
		time.Sleep(s.unavailableRetryInterval)
	}
}

func (s *StateMachine[T]) GetState() (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.state, nil
}

func (s *StateMachine[T]) createKey(sequence int) string {
	return fmt.Sprintf("%s-%09d", s.keyPrefix, sequence)
}
