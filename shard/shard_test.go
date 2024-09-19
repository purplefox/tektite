package shard

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	stateUpdatorBucketName = "state-updator-bucket"
	stateUpdatorKeyprefix  = "state-updator-keyprefix"
	dataBucketName         = "data-bucket"
	dataKeyprefix          = "data-keyprefix"
)

func TestApplyChanges(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	shard := NewLsmShard(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.ManagerOpts{})
	err := shard.Start()
	require.NoError(t, err)

	testApplyChanges(t, shard, []byte(uuid.New().String()))
}

func TestApplyChangesRestart(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	shard := NewLsmShard(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.ManagerOpts{})
	err := shard.Start()
	require.NoError(t, err)

	tableID := []byte(uuid.New().String())
	testApplyChanges(t, shard, tableID)

	err = shard.Stop()
	require.NoError(t, err)

	// recreate shard
	shard = NewLsmShard(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.ManagerOpts{})
	err = shard.Start()
	require.NoError(t, err)

	// Registration should still be there - as metadata was committed to object store after previous ApplyChanges
	res, err := shard.QueryTablesInRange(nil, nil)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID := res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))
}

func testApplyChanges(t *testing.T, shard *LsmShard, tableID []byte) {
	keyStart := []byte("key000001")
	keyEnd := []byte("key000010")

	regEntry := lsm.RegistrationEntry{
		Level:      1,
		TableID:    tableID,
		MinVersion: 123,
		MaxVersion: 1235,
		KeyStart:   keyStart,
		KeyEnd:     keyEnd,
		AddedTime:  uint64(time.Now().UnixMilli()),
		NumEntries: 1234,
		TableSize:  12345567,
	}

	batch := lsm.RegistrationBatch{
		Registrations: []lsm.RegistrationEntry{regEntry},
	}
	ch := make(chan error, 1)
	err := shard.ApplyLsmChanges(batch, func(err error) error {
		ch <- err
		return nil
	})
	require.NoError(t, err)
	err = <-ch
	require.NoError(t, err)

	res, err := shard.QueryTablesInRange(keyStart, keyEnd)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, 1, len(res[0]))
	resTableID := res[0][0].ID
	require.Equal(t, tableID, []byte(resTableID))
}

func TestApplyChangesL0Full(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	shard := NewLsmShard(stateUpdatorBucketName, stateUpdatorKeyprefix, dataBucketName, dataKeyprefix, objStore,
		lsm.ManagerOpts{})
	err := shard.Start()
	require.NoError(t, err)

}
