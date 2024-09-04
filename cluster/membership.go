package cluster

import (
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

type Membership struct {
	updateInterval       time.Duration
	evictionInterval     time.Duration
	updateTimer          *time.Timer
	lock                 sync.Mutex
	started      bool
	stateMachine *StateMachine[MembershipState]
	address      string
	leader               bool
	becomeLeaderCallback func()
}

func NewMembership(bucket string, keyPrefix string, addresss string, objStoreClient objstore.Client, updateInterval time.Duration,
	evictionInterval time.Duration, becomeLeaderCallback func()) *Membership {
	return &Membership{
		address:              addresss,
		stateMachine:         NewStateMachine[MembershipState](bucket, keyPrefix, objStoreClient, StateMachineOpts{}),
		updateInterval:       updateInterval,
		evictionInterval:     evictionInterval,
		becomeLeaderCallback: becomeLeaderCallback,
	}
}

func (m *Membership) Start() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return
	}
	m.scheduleTimer()
	m.started = true
}

func (m *Membership) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return
	}
	m.started = false
	m.updateTimer.Stop()
}

func (m *Membership) scheduleTimer() {
	m.updateTimer = time.AfterFunc(m.updateInterval, m.updateOnTimer)
}

func (m *Membership) updateOnTimer() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return
	}
	if err := m.update(); err != nil {
		log.Errorf("failed to update membership: %v", err)
	}
	m.scheduleTimer()
}

func (m *Membership) update() error {
	newState, err := m.stateMachine.Update(m.updateState)
	if err != nil {
		return err
	}
	if newState.Members[0].Address == m.address {
		if !m.leader {
			m.becomeLeaderCallback()
		}
		m.leader = true
	}
	return nil
}

func (m *Membership) updateState(memberShipState MembershipState) (MembershipState, error) {
	now := time.Now().UTC().UnixMilli()
	found := false
	var newMembers []MembershipEntry
	// FIXME what if not first time and our entry has been evicted?
	for _, member := range memberShipState.Members {
		if member.Address == m.address {
			member.UpdateTime = now
			found = true
		} else {
			if now-member.UpdateTime >= m.evictionInterval.Milliseconds() {
				// member evicted
				continue
			}
		}
		newMembers = append(newMembers, member)
	}
	if !found {
		newMembers = append(newMembers, MembershipEntry{
			Address:    m.address,
			UpdateTime: now,
		})
	}
	memberShipState.Members = newMembers
	return memberShipState, nil
}

type MembershipState struct {
	Members []MembershipEntry
}

type MembershipEntry struct {
	Address    string
	UpdateTime int64
}

func (m *Membership) GetState() (MembershipState, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return MembershipState{}, errors.New("not started")
	}
	return m.stateMachine.GetState()
}
