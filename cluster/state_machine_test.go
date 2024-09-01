package cluster

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

/*
Test:
* Join an existing group after started
*/

func TestInLoop(t *testing.T) {
	for i := 0; i < 1000; i++ {
		log.Infof("iteration %d", i)
		TestLongUnavailabilityOneMember(t)
	}
}

func TestStateMachine(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		objStores[i] = objStore
	}
	runTime := 5 * time.Second
	applyLoadAndVerifyStateMachine(t, runTime, numMembers, 100*time.Millisecond, 10*time.Millisecond, objStores)
}

func TestGarbageCollection(t *testing.T) {
	prefix := "prefix1"
	address := "address1"
	objStore := dev.NewInMemStore(0)
	gcInterval := 10 * time.Millisecond
	updateInterval := 10 * time.Millisecond
	retainedDuration := 10 * updateInterval

	member := NewStateMachine[int](prefix, address, objStore, gcInterval, updateInterval)
	member.Start()
	defer member.Stop()

	for i := 0; i < 100; i++ {
		_, err := member.Update(func(state int) (int, error) {
			return state + 1, nil
		})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}
	now := time.Now().UnixMilli()

	time.Sleep(3 * gcInterval)

	infos, err := objStore.ListObjectsWithPrefix([]byte(prefix))
	require.NoError(t, err)
	// older objects should be deleted
	for _, info := range infos {
		// allow 25% leeway
		allowedRetainedMs := int64(1.25 * float64(retainedDuration.Milliseconds()))
		require.LessOrEqual(t, now-info.LastModified.UnixMilli(), allowedRetainedMs)
	}
}

// Test object store unavailability with short times < updateInterval
func TestShortUnavailabilityOneMember(t *testing.T) {
	updateInterval := 100 * time.Millisecond
	testUnavailabilityOneMember(t, updateInterval, 0, updateInterval)
}

// Test object store unavailability with longer times > retainedDuration
func TestLongUnavailabilityOneMember(t *testing.T) {
	updateInterval := 100 * time.Millisecond
	testUnavailabilityOneMember(t, updateInterval, updateInterval*RetainedDurationFactor, updateInterval*RetainedDurationFactor*2)
}

func testUnavailabilityOneMember(t *testing.T, updateInterval time.Duration, minUnavailableTime time.Duration, maxUnavailableTime time.Duration) {
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		objStores[i] = &unavailableObjStoreProxy{
			objStore: objStore,
		}
	}
	runTime := 5 * time.Second
	endTime := arista.NanoTime() + uint64(runTime.Nanoseconds())

	go func() {
		for arista.NanoTime() < endTime {
			delay := minUnavailableTime + time.Duration(rand.Intn(int(maxUnavailableTime-minUnavailableTime)))
			uav := objStores[numMembers-1].(*unavailableObjStoreProxy)
			uav.unavailable.Store(true)
			log.Infof("sleeping for %d ms", delay.Milliseconds())
			time.Sleep(delay)
			uav.unavailable.Store(false)
			time.Sleep(delay)
		}
	}()

	applyLoadAndVerifyStateMachine(t, runTime, numMembers, updateInterval, 10*time.Millisecond, objStores)
}

func TestLongUnavailabilityAllMembers(t *testing.T) {
	updateInterval := 100 * time.Millisecond
	testUnavailabilityAllMembers(t, updateInterval, updateInterval, updateInterval*2*RetainedDurationFactor)
}

func testUnavailabilityAllMembers(t *testing.T, updateInterval time.Duration, minUnavailableTime time.Duration, maxUnavailableTime time.Duration) {
	objStore := dev.NewInMemStore(0)
	numMembers := 5
	objStores := make([]objstore.Client, numMembers)
	for i := 0; i < numMembers; i++ {
		objStores[i] = &unavailableObjStoreProxy{
			objStore: objStore,
		}
	}
	runTime := 5 * time.Second
	endTime := arista.NanoTime() + uint64(runTime.Nanoseconds())

	go func() {
		for arista.NanoTime() < endTime {
			unavailTime := minUnavailableTime + time.Duration(rand.Intn(int(maxUnavailableTime-minUnavailableTime)))
			// We make all stores unavailable at same time
			for _, objStore := range objStores {
				uav := objStore.(*unavailableObjStoreProxy)
				uav.unavailable.Store(true)
			}
			time.Sleep(unavailTime)
			for _, objStore := range objStores {
				uav := objStore.(*unavailableObjStoreProxy)
				uav.unavailable.Store(false)
			}
		}
	}()

	applyLoadAndVerifyStateMachine(t, runTime, numMembers, updateInterval, 10*time.Millisecond, objStores)
}

func applyLoadAndVerifyStateMachine(t *testing.T, runTime time.Duration, numMembers int, updateInterval time.Duration, updateDelay time.Duration,
	objStores []objstore.Client) {
	prefix := "prefix1"
	gcInterval := 50 * time.Millisecond

	type stateMachineState struct {
		Val     int
		Updates []string
	}

	var members []*StateMachine[stateMachineState]
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		member := NewStateMachine[stateMachineState](prefix, address, objStores[i], gcInterval, updateInterval)
		members = append(members, member)
		members[i] = member
		member.Start()
		defer member.Stop()
	}

	endTime := arista.NanoTime() + uint64(runTime.Nanoseconds())

	var numUpdates int64

	stopChans := make([]chan error, numMembers)
	for i := 0; i < len(members); i++ {
		ch := make(chan error, 1)
		stopChans[i] = ch
		member := members[i]
		go func() {
			for arista.NanoTime() < endTime {
				_, err := member.Update(func(s stateMachineState) (stateMachineState, error) {
					s.Updates = append(s.Updates, fmt.Sprintf("update-%d", s.Val))
					s.Val++
					return s, nil
				})
				if err != nil {
					ch <- err
					return
				}
				atomic.AddInt64(&numUpdates, 1)
				time.Sleep(updateDelay)
			}
			ch <- nil
		}()
	}

	for _, ch := range stopChans {
		err := <-ch
		require.NoError(t, err)
	}

	// Do a final no-op update on each member to make sure it loads final state
	for _, member := range members {
		_, err := member.Update(func(state stateMachineState) (stateMachineState, error) {
			return state, nil
		})
		require.NoError(t, err)
	}

	totUpdates := int(atomic.LoadInt64(&numUpdates))
	for _, member := range members {
		// State machine updates should be applied serially with multiple concurrent updates
		finalState, err := member.GetState()
		require.NoError(t, err)
		if totUpdates != finalState.Val {
			log.Infof("totUpdates %d finalState.Val %d", totUpdates, finalState.Val)
			for i, update := range finalState.Updates {
				expected := fmt.Sprintf("update-%d", i)
				if expected != update {
					log.Infof("*** unexpected %s", update)
				} else {
					log.Info(update)
				}
			}
		}
		require.Equal(t, totUpdates, finalState.Val)
		require.Equal(t, totUpdates, len(finalState.Updates))
		for i, update := range finalState.Updates {
			require.Equal(t, fmt.Sprintf("update-%d", i), update)
		}
	}
	log.Infof("totUpdates is %d", totUpdates)
}

type unavailableObjStoreProxy struct {
	unavailable atomic.Bool
	objStore    objstore.Client
}

func (u *unavailableObjStoreProxy) Get(key []byte) ([]byte, error) {
	if u.unavailable.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Get(key)
}

func (u *unavailableObjStoreProxy) Put(key []byte, value []byte) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Put(key, value)
}

func (u *unavailableObjStoreProxy) PutIfNotExists(key []byte, value []byte) (bool, error) {
	if u.unavailable.Load() {
		return false, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.PutIfNotExists(key, value)
}

func (u *unavailableObjStoreProxy) Delete(key []byte) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.Delete(key)
}

func (u *unavailableObjStoreProxy) DeleteAll(keys [][]byte) error {
	if u.unavailable.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.DeleteAll(keys)
}

func (u *unavailableObjStoreProxy) ListObjectsWithPrefix(prefix []byte) ([]objstore.ObjectInfo, error) {
	if u.unavailable.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "store is unavailable")
	}
	return u.objStore.ListObjectsWithPrefix(prefix)
}

func (u *unavailableObjStoreProxy) Start() error {
	return u.objStore.Start()
}

func (u *unavailableObjStoreProxy) Stop() error {
	return u.objStore.Stop()
}
