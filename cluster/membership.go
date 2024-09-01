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
	started              bool
	protected            *StateMachine[MembershipState]
	address              string
	leader               bool
	becomeLeaderCallback func()
}

func NewMembership(keyPrefix string, addresss string, objStoreClient objstore.Client, updateInterval time.Duration,
	evictionInterval time.Duration, gcInterval time.Duration, becomeLeaderCallback func()) *Membership {
	// We set the state machine update interval to twice the member update interval, as we'll be updating periodically
	// anyway from the member, and we don't want to do unnecessary updates.
	stateMachineUpdateInterval := updateInterval * 2
	return &Membership{
		address:              addresss,
		protected:            NewStateMachine[MembershipState](keyPrefix, addresss, objStoreClient, gcInterval, stateMachineUpdateInterval),
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

func (m *Membership) scheduleTimer() {
	m.updateTimer = time.AfterFunc(m.updateInterval, m.updateOnTimer)
}

func (m *Membership) updateOnTimer() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if err := m.update(); err != nil {
		log.Errorf("failed to update membership: %v", err)
	}
	m.scheduleTimer()
}

func (m *Membership) update() error {
	newState, err := m.protected.Update(m.updateState)
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
	return m.protected.GetState()
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
