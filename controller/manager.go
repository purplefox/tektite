package controller

import (
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
)

type ShardsManager struct {
	lock                   sync.RWMutex
	address                string
	numShards              int
	stateUpdatorBucketName string
	stateUpdatorKeyPrefix  string
	dataBucketName         string
	dataKeyPrefix          string
	objStoreClient         objstore.Client
	lsmOpts                lsm.ManagerOpts
	controllers            map[int]*ShardController
	currentMembership      cluster.MembershipState
}

// TODO should we pass through all these params like this, or use a factory????
func NewShardsManager(address string, numShards int, stateUpdatorBucketName string, stateUpdatorKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, lsmOpts lsm.ManagerOpts) *ShardsManager {
	return &ShardsManager{
		address:                address,
		numShards:              numShards,
		stateUpdatorBucketName: stateUpdatorBucketName,
		stateUpdatorKeyPrefix:  stateUpdatorKeyPrefix,
		dataBucketName:         dataBucketName,
		dataKeyPrefix:          dataKeyPrefix,
		objStoreClient:         objStoreClient,
		lsmOpts:                lsmOpts,
		controllers:            map[int]*ShardController{},
	}
}

func (sm *ShardsManager) MembershipChanged(newState cluster.MembershipState) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	newControllers := make(map[int]*ShardController, len(sm.controllers))
	for shardID := 0; shardID < sm.numShards; shardID++ {
		nodeID := nodeIDForShard(shardID, len(newState.Members))
		shardAddress := newState.Members[nodeID].Address
		if shardAddress == sm.address {
			// The controller lives on this node
			controller, ok := sm.controllers[shardID]
			if ok {
				// If we already have it, then keep it
				newControllers[shardID] = controller
			} else {
				// Create and start a controller
				controller = NewShardController(sm.stateUpdatorBucketName, sm.stateUpdatorKeyPrefix, sm.dataBucketName,
					sm.dataKeyPrefix, sm.objStoreClient, sm.lsmOpts)
				if err := controller.Start(); err != nil {
					return err
				}
				newControllers[shardID] = controller
			}
		}
	}
	sm.currentMembership = newState
	return nil
}

func (sm *ShardsManager) GetController(shardID int) (*ShardController, bool) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	controller, ok := sm.controllers[shardID]
	return controller, ok
}

func nodeIDForShard(shard int, numMembers int) int {
	// round-robin
	return shard % numMembers
}

func (sm *ShardsManager) AddressForShard(shard int) (string, error) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	lms := len(sm.currentMembership.Members)
	if lms == 0 {
		return "", errors.New("no cluster membership received")
	}
	nodeID := nodeIDForShard(shard, len(sm.currentMembership.Members))
	return sm.currentMembership.Members[nodeID].Address, nil
}

func (sm *ShardsManager) ControllerClient(shard int) (ControllerAPI, error) {
	return nil, nil
}

/*
Maybe have an LSM client

The topic info will have the shard ID in it

then when we register sstables, we will know the shard ID, then we want a connection to the address

so the shard manager can maintain a map of address to connection and have a method

getConnectionForShard(shardID)
*/
