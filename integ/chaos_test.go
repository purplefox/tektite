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
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInLoop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		log.Infof("iteration %d", i)
		tz := time.AfterFunc(1*time.Minute, func() {
			//common.DumpStacks()
			panic("test took too long")
		})
		TestChaosSimpleKafkaGo(t)
		tz.Stop()
	}
}

func TestChaosSimpleKafkaGo(t *testing.T) {
	testChaos(t, NewKafkaGoProducer, NewKafkaGoConsumer, false, false, 3, 1, 4,
		10, 10, 10, 4, 4, 100)
}

func TestChaosSimpleFranz(t *testing.T) {
	testChaos(t, NewFranzProducer, NewFranzConsumer, false, false, 3, 1, 2,
		10, 10, 10, 1, 2, 100)
}

func TestChaosSimpleFranzCommitOnEachConsumed(t *testing.T) {
	testChaos(t, NewFranzProducer, NewFranzConsumer, false, false, 3, 1, 2,
		10, 10, 2, 1, 1, 1)
}

func testChaos(t *testing.T, producerFactory ProducerFactory, consumerFactory ConsumerFactory,
	serverTls bool, clientTls bool, numAgents int, numTopics int, numSenders int, numKeysPerTopic int,
	numValuesPerKeyPerBatch int, numBatches int,
	numConsumerGroups int,
	numConsumersPerGroup int, commitBatchSize int) {

	extraCommandLine := "--metadata-write-interval-ms=10 --data-write-interval-ms=10"

	agents, tearDown := startAgentsWithExtraCommandLine(t, numAgents, serverTls, clientTls,
		compress.CompressionTypeNone, compress.CompressionTypeLz4, extraCommandLine)
	defer tearDown(t)

	var topicNames []string
	for i := 0; i < numTopics; i++ {
		topicName := fmt.Sprintf("chaos-topic-%s", uuid.New().String())
		createTopic(t, topicName, 10, agents[0].kafkaListenAddress, serverTls, clientTls)
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
	defer func() {
		for _, s := range senders {
			err := s.stop()
			require.NoError(t, err)
		}
	}()

	var fetchers []*fetcher
	var groupStates []*consumerGroupState
	for i := 0; i < numConsumerGroups; i++ {
		consumerGroup := fmt.Sprintf("consumer-group-%d", i)
		groupState := &consumerGroupState{
			groupID:      strings.ToUpper(consumerGroup),
			lastKeysMap:  map[string]int{},
			totToConsume: numSenders * numBatches * numKeysPerTopic * numValuesPerKeyPerBatch,
		}
		groupStates = append(groupStates, groupState)
		for j := 0; j < numConsumersPerGroup; j++ {
			bootStrapAddress := agents[rand.Intn(numAgents)].kafkaListenAddress
			consumer := createConsumerForChaos(t, consumerFactory, bootStrapAddress, consumerGroup, serverTls, clientTls)
			// Every consumer is subscribed to all topics
			for _, topicName := range topicNames {
				err := consumer.Subscribe(topicName)
				require.NoError(t, err)
			}
			f := &fetcher{
				consumer:        consumer,
				stopWg:          sync.WaitGroup{},
				groupState:      groupState,
				commitBatchSize: commitBatchSize,
			}
			fetchers = append(fetchers, f)
		}
	}

	defer func() {
		for _, f := range fetchers {
			err := f.stop()
			require.NoError(t, err)
		}
	}()

	// Start the senders
	for _, s := range senders {
		s.start()
	}

	// Start the fetchers
	for _, f := range fetchers {
		f.start()
	}

	for _, gs := range groupStates {
		gs.startLogger()
	}

	defer func() {
		for _, gs := range groupStates {
			gs.stopTimer()
		}
	}()

	//log.Infof("waiting for complete")

	for _, s := range senders {
		s.waitComplete()
	}

	//log.Infof("senders complete")

	for _, f := range fetchers {
		f.waitComplete()
	}

	//log.Infof("fetchers complete")
}

func createConsumerForChaos(t *testing.T, factory ConsumerFactory, address string, groupID string,
	serverTls bool, clientTls bool) Consumer {
	clientKey := ""
	clientCert := ""
	if clientTls {
		clientKey = clientKeyPath
		clientCert = clientCertPath
	}
	consumer, err := factory(address, groupID, serverTls, serverCertPath, clientCert, clientKey, "az1")
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
		//start := time.Now()
		if err := p.producer.Produce(topicProduces...); err != nil {
			panic(fmt.Sprintf("produce failed: %v", err))
		}
		//log.Infof("produce took %d ms", time.Now().Sub(start).Milliseconds())
		//log.Infof("sender sent batch")
	}
}

type fetcher struct {
	lock            sync.Mutex
	consumer        Consumer
	stopWg          sync.WaitGroup
	groupState      *consumerGroupState
	commitBatchSize int
}

func (p *fetcher) start() {
	p.stopWg.Add(1)
	go p.loop()
}

func (p *fetcher) waitComplete() {
	p.stopWg.Wait()
}

func (p *fetcher) stop() error {
	return p.consumer.Close()
}

func (p *fetcher) loop() {
	if err := p.loop0(); err != nil {
		panic(fmt.Sprintf("fetcher failed: %v", err))
		//log.Errorf("fetcher failed: %v", err)
	}
}

//var fetchLock sync.Mutex

func (p *fetcher) loop0() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer p.stopWg.Done()
	//defer func() {
	//	log.Infof("fetcher with consumer %s exiting loop", p.consumer.String())
	//}()
	count := 0
	for !p.groupState.finished() {
		//fetchLock.Lock()
		//	log.Infof("fetcher %p consumer group %s fetching", p, p.groupState.groupID)
		msg, err := p.consumer.Fetch(500 * time.Millisecond)
		//	log.Infof("fetcher %p consumer group %s fetch complete", p, p.groupState.groupID)
		if err != nil {
			log.Warnf("failed to fetch: %v", err)
			continue
			//fetchLock.Unlock()
			//return err
		}
		if msg == nil {
			//fetchLock.Unlock()
			continue
		}
		//log.Infof("fetcher for consumer %s got key %s val %s partition %d offset %d",
		//	p.consumer.String(), string(msg.Key), string(msg.Value), msg.PartInfo.PartitionID, msg.PartInfo.Offset)
		//log.Infof("%p partition %d offset %d got key %s val %s", p, msg.PartInfo.PartitionID, msg.PartInfo.Offset,
		//	string(msg.Key), string(msg.Value))
		if err := p.groupState.consumed(msg, p); err != nil {
			//fetchLock.Unlock()
			return err
		}
		//fetchLock.Unlock()
		count++
		if count >= p.commitBatchSize {
			//log.Infof("%s group - fetcher %p calling commit", p.groupState.groupID, p)
			if err := p.consumer.Commit(); err != nil {
				log.Warnf("consumer commit failed: %v", err)
				continue
			}
			//log.Infof("committed ok")
			count = 0
		}
	}
	if err := p.consumer.Commit(); err != nil {
		log.Errorf("consumer commit failed at end: %v", err)
		return err
	}
	return nil
}

type consumerGroupState struct {
	groupID       string
	lock          sync.Mutex
	lastKeysMap   map[string]int
	consumedCount int
	totToConsume  int
	logTimer      *time.Timer
}

func (c *consumerGroupState) finished() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.consumedCount >= c.totToConsume
}

func (c *consumerGroupState) startLogger() {
	c.lock.Lock()
	defer c.lock.Unlock()
	//c.scheduleLogTimer()
}

func (c *consumerGroupState) stopTimer() {
	c.lock.Lock()
	defer c.lock.Unlock()
	//c.logTimer.Stop()
}

func (c *consumerGroupState) scheduleLogTimer() {
	//log.Infof("scheduling log timer")
	c.logTimer = time.AfterFunc(5*time.Second, func() {
		//log.Infof("log timer fired")
		c.lock.Lock()
		defer c.lock.Unlock()
		if len(c.lastKeysMap) == 0 {
			log.Infof("no last keys")
		}
		for sKey, m := range c.lastKeysMap {
			log.Infof("%s group last key: %s, val: %v", c.groupID, sKey, m)
		}
		c.scheduleLogTimer()
	})
}

func (c *consumerGroupState) consumed(msg *kafka.Message, f *fetcher) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	sKey := string(msg.Key)
	lastVal, ok := c.lastKeysMap[sKey]
	if !ok {
		lastVal = -1
	}
	val, err := strconv.Atoi(string(msg.Value)[6:])
	//log.Infof("%s group consumed called key %s fetcher %p val %d lastVal %d partition %d offset %d",
	//	c.groupID, sKey, f, val, lastVal, msg.PartInfo.PartitionID, msg.PartInfo.Offset)
	if err != nil {
		return err
	}
	if val <= lastVal {
		// Duplicates are allowable when rebalance occurs
		log.Warnf("%p partition %d offset %d received duplicate expected %d got %d for key %s",
			c, msg.PartInfo.PartitionID, msg.PartInfo.Offset, lastVal+1, val, string(msg.Key))
		return nil
	} else if val != lastVal+1 {
		return errors.Errorf("%p partition %d offset %d received key out of order expected %d got %d for key %s",
			c, msg.PartInfo.PartitionID, msg.PartInfo.Offset, lastVal+1, val, string(msg.Key))
	}
	c.lastKeysMap[sKey] = val
	c.consumedCount++
	//log.Infof("consumed count is now %d", c.consumedCount)
	return nil
}
