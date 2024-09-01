package cluster

import (
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

/*
* Test Join sequential
* Test join parallel
* Test non leader evicted
* Test leader evicted - new leader chosen
* Test Leave?

 */

func TestJoinSequential(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()

	numMembers := 10
	var becomeLeaderCalledCount atomic.Int64
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberIndex := i
		memberShip := NewMembership("prefix1", address, objStore, 100*time.Millisecond, 5*time.Second, 10*time.Second, func() {
			if memberIndex != 0 {
				panic("becomeLeader callback called for wrong member")
			}
			becomeLeaderCalledCount.Add(1)
		})
		memberShip.Start()
		memberships = append(memberships, memberShip)
		log.Infof("waiting for member %d", memberIndex)
		waitForMembers(t, memberships...)
	}
	require.Equal(t, 1, int(becomeLeaderCalledCount.Load()))

	for _, membership := range memberships {
		state, err := membership.GetState()
		require.NoError(t, err)
		require.Equal(t, numMembers, len(state.Members))
		// should be in order of joining
		for i, membership2 := range memberships {
			require.Equal(t, membership2.address, state.Members[i].Address)
		}
	}
}

func TestJoinParallel(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()

	numMembers := 10
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberShip := NewMembership("prefix1", address, objStore, 100*time.Millisecond, 5 * time.Second, 10 * time.Second, func() {
		})
		memberShip.Start()
		// Randomise join time a little
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		memberships = append(memberships, memberShip)
	}

	// Wait for all members to join
	waitForMembers(t, memberships...)
}

func TestNonLeadersEvicted(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()

	numMembers := 5
	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberShip := NewMembership("prefix1", address, objStore, 100*time.Millisecond, 5 * time.Second, 10 * time.Second, func() {
		})
		memberShip.Start()
		memberships = append(memberships, memberShip)
	}
	waitForMembers(t, memberships...)

	leaderAddress := memberships[0].address

	log.Info("now stopping")
	for i := 0; i < numMembers-1; i++ {
		// choose a member at random - but not the leader
		index := 1 + rand.Intn(len(memberships)-1)
		log.Infof("stopping membership %d", index)
		// stop it
		memberships[index].Stop()
		memberships = append(memberships[:index], memberships[index+1:]...)
		// wait to be evicted
		waitForMembers(t, memberships...)
	}
	finalState, err := memberships[0].GetState()
	require.NoError(t, err)
	require.Equal(t, 1, len(finalState.Members))
	require.Equal(t, leaderAddress, finalState.Members[0].Address)
}

func TestLeaderEvicted(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	var memberships []*Membership
	defer func() {
		for _, membership := range memberships {
			membership.Stop()
		}
	}()

	numMembers := 5
	leaderCallbackCalledCounts := make([]int64, numMembers)

	for i := 0; i < numMembers; i++ {
		address := fmt.Sprintf("address-%d", i)
		memberIndex := i
		memberShip := NewMembership("prefix1", address, objStore, 100*time.Millisecond, 5 * time.Second, 10 * time.Second, func() {
			log.Infof("leader cb called for index %d", memberIndex)
			newVal := atomic.AddInt64(&leaderCallbackCalledCounts[memberIndex], 1)
			if newVal != 1 {
				panic("leader callback called too many times")
			}
		})
		memberShip.Start()
		memberships = append(memberships, memberShip)
		log.Infof("waiting for member %d", memberIndex)
		waitForMembers(t, memberships...)
	}

	waitForMembers(t, memberships...)
	require.Equal(t, 1, int(atomic.LoadInt64(&leaderCallbackCalledCounts[0])))
	for i := 1; i < numMembers; i++ {
		require.Equal(t, 0, int(atomic.LoadInt64(&leaderCallbackCalledCounts[i])))
	}

	log.Info("now stopping")
	for i := 0; i < numMembers-1; i++ {
		expectedNewLeader := memberships[1].address
		// stop current leader
		memberships[0].Stop()
		memberships = memberships[1:]
		// wait for leader to be evicted
		waitForMembers(t, memberships...)
		require.Equal(t, expectedNewLeader, memberships[0].address)
		//require.Equal(t, 1, int(atomic.LoadInt64(&leaderCallbackCalledCounts[i+1])))
	}
}

func waitForMembers(t *testing.T, memberships ...*Membership) {
	log.Infof("waiting for memberships %v", memberships)
	allAddresses := map[string]struct{}{}
	for _, membership := range memberships {
		allAddresses[membership.address] = struct{}{}
	}
	start := time.Now()
	for {
		ok := true
		for _, membership := range memberships {
			state, err := membership.GetState()
			require.NoError(t, err)
			log.Infof("address %s has state %v", membership.address, state)
			if len(state.Members) == len(memberships) {
				for _, member := range state.Members {
					_, exists := allAddresses[member.Address]
					require.True(t, exists)
				}
			} else {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().Sub(start) > 5*time.Second {
			require.Fail(t, "timed out waiting for memberships")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
