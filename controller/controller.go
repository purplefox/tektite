package controller

import (
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"sync/atomic"
)

type ShardController struct {
	lock                   sync.RWMutex
	objStore               objstore.Client
	lsmOpts                lsm.ManagerOpts
	clusteredData          *cluster.ClusteredData
	lsmManager             *lsm.Manager
	started                bool
	hasQueuedRegistrations atomic.Bool
	queuedRegistrations    []queuedRegistration
}

type queuedRegistration struct {
	regBatch       lsm.RegistrationBatch
	completionFunc func(error)
}

func NewShardController(stateMachineBucketName string, stateMachineKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, lsmOpts lsm.ManagerOpts) *ShardController {
	clusteredData := cluster.NewClusteredData(stateMachineBucketName, stateMachineKeyPrefix, dataBucketName, dataKeyPrefix,
		objStoreClient, cluster.ClusteredDataOpts{})
	return &ShardController{
		objStore:      objStoreClient,
		lsmOpts:       lsmOpts,
		clusteredData: clusteredData,
	}
}

func (m *ShardController) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	metaData, err := m.clusteredData.LoadData()
	if err != nil {
		return err
	}
	lsmManager := lsm.NewManager(m.objStore, m.maybeRetryApplies, true, false, m.lsmOpts)
	if err := lsmManager.Start(metaData); err != nil {
		return err
	}
	m.lsmManager = lsmManager
	m.started = true
	return nil
}

func (m *ShardController) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	return m.stop()
}

func (m *ShardController) stop() error {
	if err := m.lsmManager.Stop(); err != nil {
		return err
	}
	m.clusteredData.Stop()
	return nil
}

func (m *ShardController) ApplyLsmChanges(regBatch lsm.RegistrationBatch, completionFunc func(error)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		completionFunc(errors.New("not started"))
		return
	}
	ok, err := m.lsmManager.ApplyChanges(regBatch, false)
	if err != nil {
		completionFunc(err)
		return
	}
	if ok {
		m.afterApplyChanges(completionFunc)
		return
	}
	// L0 is full - queue the registration - it will be retried when there is space in L0
	m.hasQueuedRegistrations.Store(true)
	m.queuedRegistrations = append(m.queuedRegistrations, queuedRegistration{
		regBatch:       regBatch,
		completionFunc: completionFunc,
	})
}

func (m *ShardController) afterApplyChanges(completionFunc func(error)) {
	// Attempt to store the LSM state
	ok, err := m.clusteredData.StoreData(m.lsmManager.GetMasterRecordBytes())
	if err != nil {
		completionFunc(err)
	}
	if !ok {
		// Failed to apply changes as epoch has changed
		completionFunc(common.NewTektiteErrorf(common.Unavailable, "controller is not leader"))
		// Now we need to stop the controller as it's no longer valid
		if err := m.stop(); err != nil {
			log.Warnf("failed to stop controller: %v", err)
		}
	} else {
		completionFunc(nil)
	}
	return
}

func (m *ShardController) maybeRetryApplies() {
	// check atomic outside lock to reduce contention
	if !m.hasQueuedRegistrations.Load() {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	pos := 0
	var completionFuncs []func(error)
	for _, queuedReg := range m.queuedRegistrations {
		ok, err := m.lsmManager.ApplyChanges(queuedReg.regBatch, false)
		if err != nil {
			queuedReg.completionFunc(err)
			return
		}
		if ok {
			completionFuncs = append(completionFuncs, queuedReg.completionFunc)
			pos++
		} else {
			// full again
			break
		}
	}
	if pos > 0 {
		// We applied one or more queued registrations, now store the state
		ok, err := m.clusteredData.StoreData(m.lsmManager.GetMasterRecordBytes())
		if !ok {
			// Failed to store data as another controller has incremented the epoch - i.e. we are not leader any more
			err = common.NewTektiteErrorf(common.Unavailable, "controller not leader")
			// No longer leader so stop the controller
			if err := m.stop(); err != nil {
				log.Warnf("failed to stop controller: %v", err)
			}
		}
		if err != nil {
			// Send errors back to completions
			for _, cf := range completionFuncs {
				cf(err)
			}
			return
		}
		// no errors - remove the elements we successfully applied
		newQueueSize := len(m.queuedRegistrations) - pos
		if newQueueSize > 0 {
			m.queuedRegistrations = make([]queuedRegistration, len(m.queuedRegistrations)-pos)
			copy(m.queuedRegistrations, m.queuedRegistrations[pos:])
		} else {
			m.queuedRegistrations = nil
			m.hasQueuedRegistrations.Store(false)
		}
		// Call the completions
		for _, cf := range completionFuncs {
			cf(nil)
		}
	}
}
