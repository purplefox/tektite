package shard

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
Manager manages instances of LSM shards on a node. It responds to changes in the membership of the cluster and as
nodes are added or removed it calculates whether it needs to start or stop shard instances on this node.
*/
type Manager struct {
	lock                   sync.RWMutex
	started                bool
	address                string
	numShards              int
	stateUpdatorBucketName string
	stateUpdatorKeyPrefix  string
	dataBucketName         string
	dataKeyPrefix          string
	objStoreClient         objstore.Client
	connectionFactory      transport.ConnectionFactory
	transportServer        transport.Server
	lsmOpts                lsm.ManagerOpts
	shards                 map[int]*LsmShard
	currentMembership      cluster.MembershipState
}

// TODO should we pass through all these params like this, or use a factory????
func NewManager(address string, numShards int, stateUpdatorBucketName string, stateUpdatorKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, connectionFactory transport.ConnectionFactory,
	transportServer transport.Server, lsmOpts lsm.ManagerOpts) *Manager {
	return &Manager{
		address:                address,
		numShards:              numShards,
		stateUpdatorBucketName: stateUpdatorBucketName,
		stateUpdatorKeyPrefix:  stateUpdatorKeyPrefix,
		dataBucketName:         dataBucketName,
		dataKeyPrefix:          dataKeyPrefix,
		objStoreClient:         objStoreClient,
		connectionFactory:      connectionFactory,
		transportServer:        transportServer,
		lsmOpts:                lsmOpts,
		shards:                 map[int]*LsmShard{},
	}
}

func (sm *Manager) Start() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.started {
		return nil
	}
	// Register the handlers
	sm.transportServer.RegisterHandler(transport.HandlerIDShardApplyChanges, sm.handleApplyChanges)
	sm.transportServer.RegisterHandler(transport.HandlerIDShardQueryTablesInRange, sm.handleQueryTablesInRange)
	sm.started = true
	return nil
}

func (sm *Manager) Stop() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return nil
	}
	for _, shard := range sm.shards {
		if err := shard.Stop(); err != nil {
			return err
		}
	}
	sm.shards = map[int]*LsmShard{}
	sm.currentMembership = cluster.MembershipState{}
	sm.started = false
	return nil
}

// MembershipChanged is called when membership of the cluster changes. The manager will now create or stop LSM shards
// as appropriate
func (sm *Manager) MembershipChanged(newState cluster.MembershipState) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if !sm.started {
		return errors.New("manager not started")
	}
	newShards := make(map[int]*LsmShard, len(sm.shards))
	for shardID := 0; shardID < sm.numShards; shardID++ {
		nodeID := nodeIDForShard(shardID, len(newState.Members))
		shardAddress := newState.Members[nodeID].Address
		if shardAddress == sm.address {
			// The controller lives on this node
			shard, ok := sm.shards[shardID]
			if ok {
				// If we already have it, then keep it
				newShards[shardID] = shard
			} else {
				// Create and start a controller
				shard = NewLsmShard(sm.stateUpdatorBucketName, sm.stateUpdatorKeyPrefix, sm.dataBucketName,
					sm.dataKeyPrefix, sm.objStoreClient, sm.lsmOpts)
				if err := shard.Start(); err != nil {
					return err
				}
				newShards[shardID] = shard
			}
		}
	}
	sm.currentMembership = newState
	return nil
}

func (sm *Manager) getShard(shardID int) (*LsmShard, bool) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	controller, ok := sm.shards[shardID]
	return controller, ok
}

func nodeIDForShard(shard int, numMembers int) int {
	// round-robin
	return shard % numMembers
}

func (sm *Manager) addressForShard(shard int) (string, error) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	lms := len(sm.currentMembership.Members)
	if lms == 0 {
		return "", errors.New("no cluster membership received")
	}
	nodeID := nodeIDForShard(shard, len(sm.currentMembership.Members))
	return sm.currentMembership.Members[nodeID].Address, nil
}

func (sm *Manager) Client(shardID int) Client {
	return &client{
		m:           sm,
		shardID:     shardID,
		connFactory: sm.connectionFactory,
	}
}

func (sm *Manager) handleApplyChanges(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req ApplyChangesRequest
	req.Deserialize(request, 2)
	shard, err := sm.getShardWithError(req.ShardID)
	if err != nil {
		return err
	}
	return shard.ApplyLsmChanges(req.RegBatch, func(err error) error {
		if err != nil {
			return responseWriter(nil, err)
		}
		// Send back zero byte to represent nil OK response
		responseBuff = append(responseBuff, 0)
		return responseWriter(responseBuff, nil)
	})
}

func (sm *Manager) handleQueryTablesInRange(request []byte, responseBuff []byte, responseWriter transport.ResponseWriter) error {
	if err := checkRPCVersion(request); err != nil {
		return err
	}
	var req QueryTablesInRangeRequest
	req.Deserialize(request, 2)
	shard, err := sm.getShardWithError(req.ShardID)
	if err != nil {
		return err
	}
	res, err := shard.QueryTablesInRange(req.KeyStart, req.KeyEnd)
	if err != nil {
		return responseWriter(nil, err)
	}
	// TODO response struct?
	responseBuff = res.Serialize(responseBuff)
	return responseWriter(responseBuff, nil)
}

func (sm *Manager) getShardWithError(shardID int) (*LsmShard, error) {
	shard, ok := sm.getShard(shardID)
	if !ok {
		// Shard not found - most likely membership changed. We send back an error and the client will retry
		return nil, common.NewTektiteErrorf(common.Unavailable, "shard %d not found", shardID)
	}
	return shard, nil
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}
