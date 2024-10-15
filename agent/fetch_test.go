package agent

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"math"
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
		ForgottenTopicsData: nil,
		RackId:              nil,
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
