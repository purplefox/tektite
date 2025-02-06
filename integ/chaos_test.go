package integ

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/compress"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInLoop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		log.Infof("iteration %d", i)
		TestChaosSimple(t)
	}
}

func TestChaosSimple(t *testing.T) {
	testChaos(t, NewKafkaGoProducer, NewKafkaGoConsumer, false, false, 1, 2, 2,
		10, 10, 10, 2, 2)
}

func testChaos(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory,
	serverTls bool, clientTls bool, numAgents int, numTopics int, numSenders int, numKeysPerTopic int,
	numValuesPerKeyPerBatch int, numBatches int,
	numConsumerGroups int,
	numConsumersPerGroup int) {

	agents, tearDown := startAgents(t, numAgents, serverTls, clientTls, compress.CompressionTypeNone, compress.CompressionTypeLz4)
	defer tearDown(t)

	var topicNames []string
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("chaos-topic-%s", uuid.New().String())
		createTopic(t, topicName, 100, agents[0].kafkaListenAddress, serverTls, clientTls)
		topicNames = append(topicNames, topicName)
	}

	var senders []*sender
	for i := 0; i < numSenders; i++ {
		bootStrapAddress := agents[rand.Intn(numAgents)].kafkaListenAddress
		producer := createProducer(t, producerFactory, bootStrapAddress, serverTls, clientTls, compress.CompressionTypeNone)
		senders = append(senders, &sender{
			id:                      i,
			topicNames:              topicNames,
			numKeysPerTopic:         numKeysPerTopic,
			numValuesPerKeyPerBatch: numValuesPerKeyPerBatch,
			numBatches:              numBatches,
			producer:                producer,
		})
	}

	consumerGroupMap := map[string][]*fetcher{}
	for i := 0; i < numConsumerGroups; i++ {
		consumerGroup := fmt.Sprintf("consumer-group-%d", i)
		var totCount int64
		var fetchers []*fetcher
		for j := 0; j < numConsumersPerGroup; j++ {
			bootStrapAddress := agents[rand.Intn(numAgents)].kafkaListenAddress
			consumer := createConsumerForChaos(t, consumerFactory, bootStrapAddress, consumerGroup, serverTls, clientTls)
			// Every consumer is subscribed to all topics
			for _, topicName := range topicNames {
				err := consumer.Subscribe(topicName)
				require.NoError(t, err)
			}
			f := &fetcher{
				consumer:    consumer,
				stopWg:      sync.WaitGroup{},
				totCount:    &totCount,
				totMessages: int64(numSenders * numBatches * numKeysPerTopic * numValuesPerKeyPerBatch),
			}
			fetchers = append(fetchers, f)
		}
		consumerGroupMap[consumerGroup] = fetchers
	}

	// Start the senders
	for _, s := range senders {
		s.start()
	}

	// Start the fetchers
	for _, fetchers := range consumerGroupMap {
		for _, f := range fetchers {
			f.start()
		}
	}

	log.Infof("waiting for complete")

	for _, s := range senders {
		s.waitComplete()
	}

	for _, fetchers := range consumerGroupMap {
		for _, f := range fetchers {
			f.waitComplete()
		}
	}

}

func createConsumerForChaos(t *testing.T, factory ConsumerFactory, address string, groupID string,
	serverTls bool, clientTls bool) Consumer {
	clientKey := ""
	clientCert := ""
	if clientTls {
		clientKey = clientKeyPath
		clientCert = clientCertPath
	}
	consumer, err := factory(address, groupID, serverTls, serverCertPath, clientCert, clientKey)
	require.NoError(t, err)
	return consumer
}

type sender struct {
	lock                    sync.Mutex
	id                      int
	topicNames              []string
	numKeysPerTopic         int
	numValuesPerKeyPerBatch int
	numBatches              int
	producer                Producer
	stopWg                  sync.WaitGroup
}

func (p *sender) start() {
	p.stopWg.Add(1)
	go p.loop()
}

func (p *sender) stop() error {
	p.stopWg.Wait()
	return p.producer.Close()
}

func (p *sender) waitComplete() {
	p.stopWg.Wait()
}

func (p *sender) loop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer p.stopWg.Done()
	valueIndex := 0
	for i := 0; i < p.numBatches; i++ {
		var topicProduces []TopicProduce
		for _, topicName := range p.topicNames {
			var msgs []kafka.Message
			for j := 0; j < p.numValuesPerKeyPerBatch; j++ {
				for k := 0; k < p.numKeysPerTopic; k++ {
					key := fmt.Sprintf("key-%s-%05d-%05d", topicName, p.id, k)
					value := fmt.Sprintf("value-%05d", valueIndex+j)
					msgs = append(msgs, kafka.Message{
						Key:       []byte(key),
						Value:     []byte(value),
						TimeStamp: time.Now(),
					})
				}
			}
			topicProduces = append(topicProduces, TopicProduce{
				TopicName: topicName,
				Messages:  msgs,
			})
		}
		valueIndex += p.numValuesPerKeyPerBatch
		if err := p.producer.Produce(topicProduces...); err != nil {
			panic(fmt.Sprintf("produce failed: %v", err))
		}
		log.Infof("sender sent batch")
	}
}

type fetcher struct {
	lock        sync.Mutex
	consumer    Consumer
	totCount    *int64
	totMessages int64
	stopWg      sync.WaitGroup
}

func (p *fetcher) start() {
	p.stopWg.Add(1)
	go p.loop()
}

func (p *fetcher) waitComplete() {
	p.stopWg.Wait()
}

func (p *fetcher) loop() {
	if err := p.loop0(); err != nil {
		panic(fmt.Sprintf("fetcher failed: %v", err))
	}
}

func (p *fetcher) loop0() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer p.stopWg.Done()
	lastKeysMap := map[string]int{}
	for atomic.LoadInt64(p.totCount) < p.totMessages {
		msg, err := p.consumer.Fetch(500 * time.Millisecond)
		if err != nil {
			return err
		}
		if msg == nil {
			continue
		}
		sKey := string(msg.Key)
		lastVal, ok := lastKeysMap[sKey]
		if !ok {
			lastVal = -1
		}
		val, err := strconv.Atoi(string(msg.Value)[6:])
		if err != nil {
			return err
		}
		lastKeysMap[sKey] = val
		//log.Infof("got key %s val %d lastVal %d partition %d offset %d", sKey, val, lastVal,
		//	msg.PartInfo.PartitionID, msg.PartInfo.Offset)
		if val != lastVal+1 {
			return errors.Errorf("received key out of order expected %d got %d for key %s", lastVal+1, val, string(msg.Key))
		}
		atomic.AddInt64(p.totCount, 1)
	}
	return nil
}
