package controller

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/lsm"
	"sync"
)

type ControllerClient interface {
	LsmClient() lsm.Client
	GroupCoordinatorClient() GroupCoordinator
	DDLExecutorClient() DDLExecutor
}

type ShardControllerAPI interface {
	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	GetOffset(topicID int, partitionID int) (int64, error)
}

type GroupCoordinator interface {
	// JoinGroup, Sync, etc
}

type DDLExecutor interface {
	// Methods to execute DDL - create stream, delete stream etc
}

type ClientFactory struct {
	lock         sync.Mutex
	currentState cluster.MembershipState
}

func (cf *ClientFactory) MembershipStateChanged(state cluster.MembershipState) {
	cf.lock.Lock()
	defer cf.lock.Unlock()
	cf.currentState = state
}

func (cf *ClientFactory) CreateClient(shardID int) (ControllerClient, error) {
	// looks up cached client
	// else finds address for shardID
	// creates a connection
}

type Client struct {
}

func (c *Client) LsmClient() lsm.Client {
	//TODO implement me
	panic("implement me")
}

func (c *Client) GroupCoordinatorClient() GroupCoordinator {
	//TODO implement me
	panic("implement me")
}

func (c *Client) DDLExecutorClient() DDLExecutor {
	//TODO implement me
	panic("implement me")
}
