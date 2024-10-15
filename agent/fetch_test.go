package agent

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"sync"
	"testing"
)

func TestFetchSimple(t *testing.T) {
	topicName := "test-topic-1"
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	agent, _, tearDown := setupAgent(t, topicInfos, cfg)
	defer tearDown(t)

	address := agent.Conf().KafkaListenerConfig.Address
	batch := produceBatch(t, topicName, partitionID, address)

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	fetchOffset := 0

	fetchReq := kafkaprotocol.FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  math.MaxInt32,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(partitionID),
						FetchOffset:       int64(fetchOffset),
						PartitionMaxBytes: math.MaxInt32,
					},
				},
			},
		},
	}

	fetchResp := kafkaprotocol.FetchResponse{}

	r, err := conn.SendRequest(&fetchReq, kafkaprotocol.APIKeyFetch, 4, &fetchResp)
	res, ok := r.(*kafkaprotocol.FetchResponse)
	require.True(t, ok)

	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(res.ErrorCode))
	require.Equal(t, 1, len(res.Responses))
	topicResp := res.Responses[0]
	require.Equal(t, topicName, *topicResp.Topic)
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(partResp.ErrorCode))
	receivedBatches := partResp.Records
	require.Equal(t, 1, len(receivedBatches))

	require.Equal(t, batch, receivedBatches[0])

}

func TestFetchComplex(t *testing.T) {
	numAgents := 2

	topicName := "test-topic-1"
	partitionID := 12
	topicInfos := []topicmeta.TopicInfo{
		{
			Name:           topicName,
			PartitionCount: 100,
		},
	}
	cfg := NewConf()
	var agents []*Agent
	var tearDowns []func(*testing.T)
	for i := 0; i < numAgents; i++ {
		agent, _, tearDown := setupAgent(t, topicInfos, cfg)
		agents = append(agents, agent)
		tearDowns = append(tearDowns, tearDown)
	}
	defer func() {
		for _, tearDown := range tearDowns {
			tearDown(t)
		}
	}()

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)
	var connections []*KafkaApiConnection
	for _, agent := range agents {
		conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
		require.NoError(t, err)
		connections = append(connections, conn)
	}
	defer func() {
		for _, conn := range connections {
			err := conn.Close()
			require.NoError(t, err)
		}
	}()

	numFetchers := 1
	var fetchRunners []*fetchRunner
	numBatches := 10

	for i := 0; i < numFetchers; i++ {
		runner := &fetchRunner{
			topicName:   topicName,
			partitionID: partitionID,
			numBatches:  numBatches,
			maxBytes:    1000,
			maxWaitMs:   500,
			connections: connections,
		}
		fetchRunners = append(fetchRunners, runner)
		runner.start()
	}

	prodRunner := &produceRunner{
		topicName:       topicName,
		partitionID:     partitionID,
		numBatches:      numBatches,
		connections:     connections,
		offset:          0,
		recordsPerBatch: 100,
	}
	prodRunner.start()
	prodRunner.waitComplete()

	producedBatches := prodRunner.getBatches()

	for _, runner := range fetchRunners {
		runner.waitComplete()
		require.Equal(t, producedBatches, runner.getBatches())
	}
}

type produceRunner struct {
	topicName       string
	partitionID     int
	numBatches      int
	stopWg          sync.WaitGroup
	connections     []*KafkaApiConnection
	batches         [][]byte
	offset          int64
	recordsPerBatch int
}

func (p *produceRunner) start() {
	p.stopWg.Add(1)
	go p.loop()
}

func (p *produceRunner) waitComplete() {
	p.stopWg.Wait()
}

func (p *produceRunner) getBatches() [][]byte {
	return p.batches
}

func (p *produceRunner) loop() {
	defer p.stopWg.Done()
	for len(p.batches) < p.numBatches {
		p.produce()
	}
}

func (p *produceRunner) produce() {
	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(int(p.offset), p.recordsPerBatch)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(p.topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(p.partitionID),
						Records: [][]byte{
							batch,
						},
					},
				},
			},
		},
	}
	p.offset += int64(p.recordsPerBatch)

	resp := p.sendProduce(&req)
	partResp := resp.Responses[0].PartitionResponses[0]
	if partResp.ErrorCode != kafkaprotocol.ErrorCodeNone {
		panic(fmt.Sprintf("produce returned error code %d", partResp.ErrorCode))
	}
	p.batches = append(p.batches, batch)
	log.Infof("produced batch of size %d", len(batch))
}

func (f *produceRunner) sendProduce(req *kafkaprotocol.ProduceRequest) *kafkaprotocol.ProduceResponse {
	index := rand.Intn(len(f.connections))
	conn := f.connections[index]
	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyProduce, 3, &resp)
	if err != nil {
		panic(fmt.Sprintf("failed to send produce request: %v", err))
	}
	res := r.(*kafkaprotocol.ProduceResponse)
	return res
}

type fetchRunner struct {
	topicName   string
	partitionID int
	numBatches  int
	maxBytes    int
	maxWaitMs   int
	stopWg      sync.WaitGroup
	connections []*KafkaApiConnection

	fetchOffset int64
	batches     [][]byte
}

func (f *fetchRunner) start() {
	f.stopWg.Add(1)
	go f.loop()
}

func (f *fetchRunner) waitComplete() {
	f.stopWg.Wait()
}

func (f *fetchRunner) getBatches() [][]byte {
	return f.batches
}

func (f *fetchRunner) loop() {
	defer f.stopWg.Done()
	for len(f.batches) < f.numBatches {
		f.fetch()
	}
}

func (f *fetchRunner) fetch() {

	maxBytes := int32(1 + rand.Intn(f.maxBytes))

	maxWait := int32(rand.Intn(f.maxWaitMs))

	minBytes := maxBytes / 2

	fetchReq := kafkaprotocol.FetchRequest{
		MaxWaitMs: maxWait,
		MinBytes:  minBytes,
		MaxBytes:  maxBytes,
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(f.topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:         int32(f.partitionID),
						FetchOffset:       f.fetchOffset,
						PartitionMaxBytes: math.MaxInt32,
					},
				},
			},
		},
	}
	resp := f.sendFetch(&fetchReq)

	partResp := resp.Responses[0].Partitions[0]
	if partResp.ErrorCode != kafkaprotocol.ErrorCodeNone {
		panic(fmt.Sprintf("fetch got error %d", partResp.ErrorCode))
	}
	if len(partResp.Records) > 0 {
		f.batches = append(f.batches, partResp.Records...)
		numRecords := kafkaencoding.NumRecords(partResp.Records[len(partResp.Records)-1])
		f.fetchOffset += int64(numRecords)
		log.Infof("fetchrunner got batch, offset is now %d", f.fetchOffset)
	}
}

func (f *fetchRunner) sendFetch(req *kafkaprotocol.FetchRequest) *kafkaprotocol.FetchResponse {
	index := rand.Intn(len(f.connections))
	conn := f.connections[index]
	var resp kafkaprotocol.FetchResponse
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyFetch, 4, &resp)
	if err != nil {
		panic(fmt.Sprintf("failed to send fetch request: %v", err))
	}
	res := r.(*kafkaprotocol.FetchResponse)
	return res
}

func produceBatch(t *testing.T, topicName string, partitionID int, address string) []byte {

	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 100)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: nil,
		Acks:            -1,
		TimeoutMs:       1234,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index: int32(partitionID),
						Records: [][]byte{
							batch,
						},
					},
				},
			},
		},
	}

	cl, err := NewKafkaApiClient()
	require.NoError(t, err)

	conn, err := cl.NewConnection(address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	var resp kafkaprotocol.ProduceResponse
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, &resp)
	produceResp, ok := r.(*kafkaprotocol.ProduceResponse)
	require.True(t, ok)

	require.Equal(t, 1, len(produceResp.Responses))
	require.Equal(t, 1, len(produceResp.Responses[0].PartitionResponses))
	partResp := produceResp.Responses[0].PartitionResponses[0]
	require.Equal(t, int16(kafkaprotocol.ErrorCodeNone), partResp.ErrorCode)
	require.Equal(t, (*string)(nil), partResp.ErrorMessage)

	return batch
}
