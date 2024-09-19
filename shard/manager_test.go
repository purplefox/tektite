package shard

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestManagerShardCreation(t *testing.T) {
	localTransports := transport.NewLocalTransports()

	numShards := 10
	numMembers := 3
	var addresses []string
	var managers []*Manager
	for i := 0; i < numMembers; i++ {
		address := uuid.New().String()
		addresses = append(addresses, address)
		transportServer, err := localTransports.NewLocalServer(address)
		require.NoError(t, err)
		objStore := dev.NewInMemStore(0)
		mgr := NewManager(numShards, stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix,
			objStore, localTransports.CreateConnection, transportServer, lsm.ManagerOpts{})
		err = mgr.Start()
		require.NoError(t, err)
		managers = append(managers, mgr)
	}
	defer func() {
		for _, mgr := range managers {
			err := mgr.Stop()
			require.NoError(t, err)
		}
	}()

	now := time.Now().UnixMilli()
	newState := cluster.MembershipState{
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				Address:    addresses[0],
				UpdateTime: now,
			},
			{
				Address:    addresses[1],
				UpdateTime: now,
			},
			{
				Address:    addresses[2],
				UpdateTime: now,
			},
		},
	}
	for _, mgr := range managers {
		err := mgr.MembershipChanged(newState)
		require.NoError(t, err)
	}

	checkMapping(t, managers, numShards, newState.Members)

	// Now remove a member
	now = time.Now().UnixMilli()
	newState = cluster.MembershipState{
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				Address:    addresses[1],
				UpdateTime: now,
			},
			{
				Address:    addresses[2],
				UpdateTime: now,
			},
		},
	}
	checkMapping(t, managers, numShards, newState.Members)

	/*
		TODO also need to test with reducing to zero members
	*/
}

func checkMapping(t *testing.T, managers []*Manager, numShards int, members []cluster.MembershipEntry) {
	mapping := expectedShardMemberMapping(numShards, members)
	for shard, address := range mapping {
		for _, mgr := range managers {
			if address == mgr.transportServer.Address() {
				// shard must be on this member
				s, ok := mgr.getShard(shard)
				require.True(t, ok)
				require.NotNil(t, s)
			} else {
				// must not be here
				s, ok := mgr.getShard(shard)
				require.False(t, ok)
				require.Nil(t, s)
			}
		}
	}
}

func expectedShardMemberMapping(numShards int, members []cluster.MembershipEntry) map[int]string {
	mapping := map[int]string{}
	for shard := 0; shard < numShards; shard++ {
		i := shard % len(members)
		mapping[shard] = members[i].Address
	}
	return mapping
}

/*
Test client unavailability
Test with unknown shard
Test changing membership and shards being added/removed
*/
