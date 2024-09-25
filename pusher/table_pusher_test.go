package pusher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"hash/crc32"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	common.EnableTestPorts()
}

func TestTablePusherHandleProduceBatchSimple(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID := 1234
	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID, PartitionCount: 20},
	}}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	msgs := []rawKafkaMessage{
		{
			timestamp: time.Now().UnixMilli(),
			key:       []byte("key1"),
			value:     []byte("val1"),
		},
	}
	recordBatch := createRecordBatch(msgs, 0)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)
	checkNoPartitionResponseErrors(t, respCh, &req)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		require.Equal(t, recordBatch, kv.Value)
	}

	// check that table has been registered with LSM

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	require.Equal(t, 1, len(receivedReg.Registrations))

	reg := receivedReg.Registrations[0]
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherHandleProduceBatchMultipleTopicsAndPartitions(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID1 := 1234
	topicID2 := 4321

	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{
			topicID1: {
				12: 1002,
				7:  32,
			},
			topicID2: {
				23: 564,
			},
		},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID1, PartitionCount: 20},
		"topic2": {TopicID: topicID2, PartitionCount: 30},
	}}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch1 := createBatchWithIncrementingKVs(10)
	recordBatch2 := createBatchWithIncrementingKVs(15)
	recordBatch3 := createBatchWithIncrementingKVs(20)
	recordBatch4 := createBatchWithIncrementingKVs(25)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
					{
						Index: 12,
						Records: [][]byte{
							recordBatch2,
						},
					},
					{
						Index: 7,
						Records: [][]byte{
							recordBatch3,
						},
					},
				},
			},
			{
				Name: strPtr("topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							recordBatch4,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	checkNoPartitionResponseErrors(t, respCh, &req)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)

	var receivedKVs []common.KV
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		receivedKVs = append(receivedKVs, kv)
	}
	require.Equal(t, 4, len(receivedKVs))

	var expectedKVs []common.KV

	partitionID := int(req.TopicData[0].PartitionData[0].Index)
	expectedKey1, err := createExpectedKey(topicID1, partitionID, 1002)
	require.NoError(t, err)
	expectedKey2, err := createExpectedKey(topicID1, partitionID, 1002+10)
	require.NoError(t, err)

	partitionID = int(req.TopicData[0].PartitionData[2].Index)
	expectedKey3, err := createExpectedKey(topicID1, partitionID, 32)
	require.NoError(t, err)

	partitionID = int(req.TopicData[1].PartitionData[0].Index)
	expectedKey4, err := createExpectedKey(topicID2, partitionID, 564)
	require.NoError(t, err)

	expectedKVs = append(expectedKVs, common.KV{
		Key:   expectedKey1,
		Value: recordBatch1,
	}, common.KV{
		Key:   expectedKey2,
		Value: recordBatch2,
	}, common.KV{
		Key:   expectedKey3,
		Value: recordBatch3,
	}, common.KV{
		Key:   expectedKey4,
		Value: recordBatch4,
	})

	slices.SortFunc(expectedKVs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})

	require.Equal(t, expectedKVs, receivedKVs)

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	require.Equal(t, 1, len(receivedReg.Registrations))

	reg := receivedReg.Registrations[0]
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherPushWhenBufferIsFull(t *testing.T) {
	batch1 := createBatchWithIncrementingKVs(100)
	batch2 := createBatchWithIncrementingKVs(100)

	// So it won't write after receiving batch1 but will write after batch2
	bufferMaxSize := len(batch1) + len(batch2) - 10

	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Hour // So it doesn't push on timer
	cfg.BufferMaxSizeBytes = bufferMaxSize
	objStore := dev.NewInMemStore(0)
	topicID := 1234

	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID, PartitionCount: 30},
	}}

	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	req1 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							batch1,
						},
					},
				},
			},
		},
	}
	var batch1Complete atomic.Bool
	respCh1 := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req1, func(resp *kafkaprotocol.ProduceResponse) error {
		batch1Complete.Store(true)
		respCh1 <- resp
		return nil
	})
	require.NoError(t, err)

	// Wait a little bit
	time.Sleep(50 * time.Millisecond)
	// Should not have been written
	require.False(t, batch1Complete.Load())

	// Now send batch2

	req2 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							batch2,
						},
					},
				},
			},
		},
	}
	respCh2 := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req2, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh2 <- resp
		return nil
	})
	require.NoError(t, err)

	// They should now both complete
	checkNoPartitionResponseErrors(t, respCh1, &req1)

	checkNoPartitionResponseErrors(t, respCh2, &req2)

	// check that 1 table has been pushed to object store
	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	require.Equal(t, 2, ssTables[0].NumEntries())

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	require.Equal(t, 1, len(receivedReg.Registrations))

	reg := receivedReg.Registrations[0]
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherPushWhenTimeoutIsExceeded(t *testing.T) {
	batch := createBatchWithIncrementingKVs(100)

	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 250 * time.Millisecond
	cfg.BufferMaxSizeBytes = math.MaxInt
	objStore := dev.NewInMemStore(0)
	topicID := 1234

	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID, PartitionCount: 20},
	}}

	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)

	start := time.Now()
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							batch,
						},
					},
				},
			},
		},
	}
	var batchComplete atomic.Bool
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		batchComplete.Store(true)
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	time.Sleep(cfg.WriteTimeout / 2)
	// Should not have been written
	require.False(t, batchComplete.Load())

	// Wait until it completes
	testutils.WaitUntil(t, func() (bool, error) {
		return batchComplete.Load(), nil
	})
	require.True(t, time.Since(start) >= cfg.WriteTimeout)

	checkNoPartitionResponseErrors(t, respCh, &req)

	// check that 1 table has been pushed to object store
	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))
	require.Equal(t, 1, ssTables[0].NumEntries())

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	require.Equal(t, 1, len(receivedReg.Registrations))

	reg := receivedReg.Registrations[0]
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherHandleProduceBatchMixtureErrorsAndSuccesses(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := dev.NewInMemStore(0)
	topicID1 := 1234
	topicID2 := 4321

	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{
			topicID1: {
				12: 1002,
				7:  32,
			},
			topicID2: {
				23: 564,
			},
		},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID1, PartitionCount: 20},
		"topic2": {TopicID: topicID2, PartitionCount: 30},
	}}

	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)

	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	recordBatch1 := createBatchWithIncrementingKVs(10)
	recordBatch2 := createBatchWithIncrementingKVs(15)
	recordBatch3 := createBatchWithIncrementingKVs(20)
	recordBatch4 := createBatchWithIncrementingKVs(25)

	recordBatch5 := createBatchWithIncrementingKVs(10)
	recordBatch6 := createBatchWithIncrementingKVs(10)
	recordBatch7 := createBatchWithIncrementingKVs(10)
	recordBatch8 := createBatchWithIncrementingKVs(10)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
					{
						Index: 12,
						Records: [][]byte{
							recordBatch2,
						},
					},
					{
						Index: 999, // unknown
						Records: [][]byte{
							recordBatch8,
						},
					},
					{
						Index: 7,
						Records: [][]byte{
							recordBatch3,
						},
					},
				},
			},
			{
				Name: strPtr("topic2"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 23,
						Records: [][]byte{
							recordBatch4,
						},
					},
				},
			},
			{
				Name: strPtr("topic_unknown"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 11,
						Records: [][]byte{
							recordBatch5,
						},
					},
					{
						Index: 26,
						Records: [][]byte{
							recordBatch6,
						},
					},
					{
						Index: 76,
						Records: [][]byte{
							recordBatch7,
						},
					},
				},
			},
		},
	}

	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	// Some of the partition produces should have succeeded and some should have failed
	noSuchTopicErr := expectedErr{errCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition, errMsg: "unknown topic: topic_unknown"}
	expected := [][]expectedErr{
		{noErr, noErr, expectedErr{errCode: kafkaprotocol.ErrorCodeUnknownTopicOrPartition, errMsg: "unknown partition: 999 for topic: topic1"}, noErr},
		{noErr},
		{noSuchTopicErr, noSuchTopicErr, noSuchTopicErr},
	}
	checkResponseErrors(t, respCh, expected)

	// check that table has been pushed to object store

	ssTables, objects := getSSTablesFromStore(t, cfg.DataBucketName, objStore)

	var receivedKVs []common.KV
	iter, err := ssTables[0].NewIterator(nil, nil)
	require.NoError(t, err)
	for {
		ok, kv, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		receivedKVs = append(receivedKVs, kv)
	}
	require.Equal(t, 4, len(receivedKVs))

	var expectedKVs []common.KV

	partitionID := int(req.TopicData[0].PartitionData[0].Index)
	expectedKey1, err := createExpectedKey(topicID1, partitionID, 1002)
	require.NoError(t, err)
	expectedKey2, err := createExpectedKey(topicID1, partitionID, 1002+10)
	require.NoError(t, err)

	partitionID = int(req.TopicData[0].PartitionData[3].Index)
	expectedKey3, err := createExpectedKey(topicID1, partitionID, 32)
	require.NoError(t, err)

	partitionID = int(req.TopicData[1].PartitionData[0].Index)
	expectedKey4, err := createExpectedKey(topicID2, partitionID, 564)
	require.NoError(t, err)

	expectedKVs = append(expectedKVs, common.KV{
		Key:   expectedKey1,
		Value: recordBatch1,
	}, common.KV{
		Key:   expectedKey2,
		Value: recordBatch2,
	}, common.KV{
		Key:   expectedKey3,
		Value: recordBatch3,
	}, common.KV{
		Key:   expectedKey4,
		Value: recordBatch4,
	})

	slices.SortFunc(expectedKVs, func(a, b common.KV) int {
		return bytes.Compare(a.Key, b.Key)
	})

	require.Equal(t, expectedKVs, receivedKVs)

	// check that table has been registered with LSM
	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
	receivedReg := receivedRegs[0]
	require.Equal(t, 1, len(receivedReg.Registrations))

	reg := receivedReg.Registrations[0]
	require.Equal(t, []byte(objects[0].Key), []byte(reg.TableID))
}

func TestTablePusherUnexpectedError(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	objStore := &failingObjectStoreClient{}
	topicID := 1234
	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID, PartitionCount: 20},
	}}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	msgs := []rawKafkaMessage{
		{
			timestamp: time.Now().UnixMilli(),
			key:       []byte("key1"),
			value:     []byte("val1"),
		},
	}
	recordBatch := createRecordBatch(msgs, 0)

	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch,
						},
					},
				},
			},
		},
	}
	respCh := make(chan *kafkaprotocol.ProduceResponse, 1)
	err = pusher.HandleProduceRequest(&req, func(resp *kafkaprotocol.ProduceResponse) error {
		respCh <- resp
		return nil
	})
	require.NoError(t, err)

	expected := [][]expectedErr{
		{{errCode: kafkaprotocol.ErrorCodeUnknownServerError}},
	}
	checkResponseErrors(t, respCh, expected)

	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 0, len(ssTables))

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 0, len(receivedRegs))
}

func TestTablePusherTemporaryUnavailability(t *testing.T) {
	cfg := NewConf()
	cfg.DataBucketName = "test-data-bucket"
	cfg.WriteTimeout = 1 * time.Millisecond // So it pushes straightaway
	cfg.AvailabilityRetryInterval = 500 * time.Millisecond
	inMemClient := dev.NewInMemStore(0)
	objStore := &unavailableObjStoreClient{
		cl: inMemClient,
	}
	topicID := 1234
	controllerClient := &testControllerClient{
		offsets: map[int]map[int]int64{},
	}
	clientFactory := func() (control.Client, error) {
		return controllerClient, nil
	}
	topicProvider := &testTopicInfoProvider{infos: map[string]*TopicInfo{
		"topic1": {TopicID: topicID, PartitionCount: 30},
	}}
	pusher, err := NewTablePusher(cfg, topicProvider, objStore, clientFactory)
	require.NoError(t, err)
	err = pusher.Start()
	require.NoError(t, err)
	defer func() {
		err := pusher.Stop()
		require.NoError(t, err)
	}()

	start := time.Now()

	// Push a couple of batches - obj store is unavailable
	msgs := []rawKafkaMessage{
		{
			timestamp: time.Now().UnixMilli(),
			key:       []byte("key1"),
			value:     []byte("val1"),
		},
	}
	recordBatch1 := createRecordBatch(msgs, 0)
	req1 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 12,
						Records: [][]byte{
							recordBatch1,
						},
					},
				},
			},
		},
	}
	respCh1 := make(chan *kafkaprotocol.ProduceResponse, 1)
	completionCalled1 := atomic.Bool{}
	err = pusher.HandleProduceRequest(&req1, func(resp *kafkaprotocol.ProduceResponse) error {
		completionCalled1.Store(true)
		respCh1 <- resp
		return nil
	})
	require.NoError(t, err)

	recordBatch2 := createRecordBatch(msgs, 0)
	req2 := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: strPtr("topic1"),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: 20,
						Records: [][]byte{
							recordBatch2,
						},
					},
				},
			},
		},
	}
	respCh2 := make(chan *kafkaprotocol.ProduceResponse, 1)
	completionCalled2 := atomic.Bool{}
	err = pusher.HandleProduceRequest(&req2, func(resp *kafkaprotocol.ProduceResponse) error {
		completionCalled2.Store(true)
		respCh2 <- resp
		return nil
	})
	require.NoError(t, err)

	time.Sleep(250 * time.Millisecond)
	// Unavailable so no completions called
	require.False(t, completionCalled1.Load())
	require.False(t, completionCalled2.Load())

	// Now make available
	objStore.available.Store(true)

	// Should now be written and complete

	checkNoPartitionResponseErrors(t, respCh1, &req1)
	checkNoPartitionResponseErrors(t, respCh2, &req2)

	require.True(t, time.Since(start) >= cfg.AvailabilityRetryInterval)

	ssTables, _ := getSSTablesFromStore(t, cfg.DataBucketName, objStore)
	require.Equal(t, 1, len(ssTables))

	receivedRegs := controllerClient.getRegistrations()
	require.Equal(t, 1, len(receivedRegs))
}

func checkNoPartitionResponseErrors(t *testing.T, respCh chan *kafkaprotocol.ProduceResponse, req *kafkaprotocol.ProduceRequest) {
	resp := <-respCh
	require.NotNil(t, resp)
	require.Equal(t, len(req.TopicData), len(resp.Responses))
	for i, topicResp := range resp.Responses {
		require.Equal(t, len(req.TopicData[i].PartitionData), len(topicResp.PartitionResponses))
		for _, pResp := range topicResp.PartitionResponses {
			var errMsg string
			if pResp.ErrorMessage != nil {
				errMsg = *pResp.ErrorMessage
			}
			require.Equalf(t, kafkaprotocol.ErrorCodeNone, int(pResp.ErrorCode),
				"expected no error but got errorCode: %d message: %s", pResp.ErrorCode, errMsg)
		}
	}
}

var noErr = expectedErr{
	errCode: kafkaprotocol.ErrorCodeNone,
}

type expectedErr struct {
	errCode int
	errMsg  string
}

func checkResponseErrors(t *testing.T, respCh chan *kafkaprotocol.ProduceResponse, expectedCodes [][]expectedErr) {
	resp := <-respCh
	require.NotNil(t, resp)
	require.Equal(t, len(expectedCodes), len(resp.Responses))
	for i, topicResp := range resp.Responses {
		require.Equal(t, len(expectedCodes[i]), len(topicResp.PartitionResponses))
		for j, pResp := range topicResp.PartitionResponses {
			var errMsg string
			if pResp.ErrorMessage != nil {
				errMsg = *pResp.ErrorMessage
			}
			expectedCode := expectedCodes[i][j].errCode
			require.Equalf(t, expectedCode, int(pResp.ErrorCode),
				"expected errorCode: %d but got: %dmsg: %s", expectedCode, pResp.ErrorCode, errMsg)
			expectedMsg := expectedCodes[i][j].errMsg
			require.Equalf(t, expectedMsg, errMsg,
				"expected errMsg: %s but got: %s", expectedMsg, errMsg)
		}
	}
}

func getSSTablesFromStore(t *testing.T, databucketName string, objStore objstore.Client) ([]*sst.SSTable, []objstore.ObjectInfo) {
	objects, err := objStore.ListObjectsWithPrefix(context.Background(), databucketName, "sst-", 10)
	require.NoError(t, err)
	var ssTables []*sst.SSTable
	for _, info := range objects {
		sstableBytes, err := objStore.Get(context.Background(), databucketName, info.Key)
		require.NoError(t, err)
		ssTable := &sst.SSTable{}
		ssTable.Deserialize(sstableBytes, 0)
		ssTables = append(ssTables, ssTable)
	}
	return ssTables, objects
}

func createExpectedKey(topicID int, partitionID int, offset int64) ([]byte, error) {
	key, err := createPartitionHash(topicID, partitionID)
	if err != nil {
		return nil, err
	}
	key = encoding.KeyEncodeInt(key, offset)
	key = encoding.EncodeVersion(key, 0)
	return key, nil
}

type testControllerClient struct {
	lock          sync.Mutex
	shardID       int
	registrations []lsm.RegistrationBatch
	offsets       map[int]map[int]int64
}

func (t *testControllerClient) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.registrations = append(t.registrations, regBatch)
	return nil
}

func (t *testControllerClient) getRegistrations() []lsm.RegistrationBatch {
	t.lock.Lock()
	defer t.lock.Unlock()
	copied := make([]lsm.RegistrationBatch, len(t.registrations))
	copy(copied, t.registrations)
	return copied
}

func (t *testControllerClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	panic("should not be called")
}

func (t *testControllerClient) GetOffsets(infos []control.GetOffsetInfo) ([]int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	var offsets []int64
	for _, info := range infos {
		partitionOffsets, ok := t.offsets[info.TopicID]
		if !ok {
			partitionOffsets = make(map[int]int64)
			t.offsets[info.TopicID] = partitionOffsets
		}
		offset, ok := partitionOffsets[info.PartitionID]
		if !ok {
			offset = 0
		}
		newOffset := info.NumOffsets + int(offset)
		partitionOffsets[info.PartitionID] = int64(newOffset)
		offsets = append(offsets, offset)
	}
	return offsets, nil
}

func (t *testControllerClient) Close() error {
	return nil
}

type testTopicInfoProvider struct {
	infos map[string]*TopicInfo
}

func (t *testTopicInfoProvider) GetTopicInfo(topicName string) (*TopicInfo, bool) {
	info, ok := t.infos[topicName]
	return info, ok
}

func strPtr(s string) *string {
	return &s
}

func createBatchWithIncrementingKVs(numMessages int) []byte {
	var msgs []rawKafkaMessage
	for i := 0; i < numMessages; i++ {
		msgs = append(msgs, rawKafkaMessage{
			timestamp: time.Now().UnixMilli(),
			key:       []byte(fmt.Sprintf("key%d", i)),
			value:     []byte(fmt.Sprintf("val%d", i)),
		})
	}
	return createRecordBatch(msgs, 0)
}

func createRecordBatch(messages []rawKafkaMessage, offsetStart int64) []byte {
	batchBytes := make([]byte, 61)
	first := true
	var firstTimestamp types.Timestamp
	var timestamp types.Timestamp
	offset := offsetStart
	for _, msg := range messages {
		var ok bool
		timestamp = types.Timestamp{Val: msg.timestamp}
		if first {
			firstTimestamp = timestamp
		}
		batchBytes, ok = kafkaencoding.AppendToBatch(batchBytes, offset, msg.key, nil, msg.value, timestamp,
			firstTimestamp, offsetStart, math.MaxInt, first)
		if !ok {
			panic("failed to append")
		}
		first = false
	}
	kafkaencoding.SetBatchHeader(batchBytes, offsetStart, offset, firstTimestamp, timestamp, len(messages), crc32.NewIEEE())
	return batchBytes
}

type rawKafkaMessage struct {
	key       []byte
	value     []byte
	headers   []byte
	timestamp int64
}

func createPartitionHash(topicID int, partitionID int) ([]byte, error) {
	kb := make([]byte, 16)
	binary.BigEndian.PutUint64(kb, uint64(topicID))
	binary.BigEndian.PutUint64(kb[8:], uint64(partitionID))
	hashFunc := sha256.New()
	if _, err := hashFunc.Write(kb); err != nil {
		return nil, err
	}
	out := hashFunc.Sum(nil)
	return out[:16], nil
}

type failingObjectStoreClient struct {
}

func (f *failingObjectStoreClient) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	panic("should not be called")
}

func (f *failingObjectStoreClient) Put(ctx context.Context, bucket string, key string, value []byte) error {
	return errors.New("some random error")
}

func (f *failingObjectStoreClient) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error) {
	panic("should not be called")
}

func (f *failingObjectStoreClient) Delete(ctx context.Context, bucket string, key string) error {
	panic("should not be called")
}

func (f *failingObjectStoreClient) DeleteAll(ctx context.Context, bucket string, keys []string) error {
	panic("should not be called")
}

func (f *failingObjectStoreClient) ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	return nil, nil
}

func (f *failingObjectStoreClient) Start() error {
	return nil
}

func (f *failingObjectStoreClient) Stop() error {
	return nil
}

type unavailableObjStoreClient struct {
	available atomic.Bool
	cl        objstore.Client
}

func (u *unavailableObjStoreClient) Get(ctx context.Context, bucket string, key string) ([]byte, error) {
	if !u.available.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Get(ctx, bucket, key)
}

func (u *unavailableObjStoreClient) Put(ctx context.Context, bucket string, key string, value []byte) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Put(ctx, bucket, key, value)
}

func (u *unavailableObjStoreClient) PutIfNotExists(ctx context.Context, bucket string, key string, value []byte) (bool, error) {
	if !u.available.Load() {
		return false, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.PutIfNotExists(ctx, bucket, key, value)
}

func (u *unavailableObjStoreClient) Delete(ctx context.Context, bucket string, key string) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.Delete(ctx, bucket, key)
}

func (u *unavailableObjStoreClient) DeleteAll(ctx context.Context, bucket string, keys []string) error {
	if !u.available.Load() {
		return common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.DeleteAll(ctx, bucket, keys)
}

func (u *unavailableObjStoreClient) ListObjectsWithPrefix(ctx context.Context, bucket string, prefix string, maxKeys int) ([]objstore.ObjectInfo, error) {
	if !u.available.Load() {
		return nil, common.NewTektiteErrorf(common.Unavailable, "object store is unavailable")
	}
	return u.cl.ListObjectsWithPrefix(ctx, bucket, prefix, maxKeys)
}

func (u *unavailableObjStoreClient) Start() error {
	return u.cl.Start()
}

func (u *unavailableObjStoreClient) Stop() error {
	return u.cl.Stop()
}
