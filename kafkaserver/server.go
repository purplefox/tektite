package kafkaserver

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaserver/protocol"

	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"io"
	"net"
	"sync"
)

const (
	readBuffSize = 8 * 1024
)

func NewServer(cfg *conf.Config, metadataProvider MetadataProvider, procProvider processorProvider,
	groupCoordinator *GroupCoordinator, streamMgr streamMgr, scramManager *auth.ScramManager) (*Server, error) {
	saslAuthManager, err := auth.NewSaslAuthManager(scramManager)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg:              cfg,
		metadataProvider: metadataProvider,
		procProvider:     procProvider,
		groupCoordinator: groupCoordinator,
		fetcher:          newFetcher(procProvider, streamMgr, int(cfg.KafkaFetchCacheMaxSizeBytes)),
		saslAuthManager:  saslAuthManager,
	}, nil
}

type Server struct {
	cfg                 *conf.Config
	listener            net.Listener
	started             bool
	lock                sync.RWMutex
	acceptLoopExitGroup sync.WaitGroup
	connections         sync.Map
	metadataProvider    MetadataProvider
	procProvider        processorProvider
	groupCoordinator    *GroupCoordinator
	fetcher             *fetcher
	listenCancel        context.CancelFunc
	saslAuthManager     *auth.SaslAuthManager
}

type processorProvider interface {
	GetProcessor(processorID int) proc.Processor
	NodeForPartition(partitionID int, mappingID string, partitionCount int) int
}

// Start - note that this does not actually start the kafka Server - we delay really starting this (with Activate)
// until the rest of the Server is started.
func (s *Server) Start() error {
	return nil
}

func (s *Server) Activate() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	list, err := s.createNetworkListener()
	if err != nil {
		return errwrap.WithStack(err)
	}
	s.listener = list
	s.started = true
	s.fetcher.start()
	s.acceptLoopExitGroup.Add(1)
	common.Go(s.acceptLoop)
	log.Debugf("started kafka Server on %s", s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID])
	return nil
}

func (s *Server) SaslAuthManager() *auth.SaslAuthManager {
	return s.saslAuthManager
}

func (s *Server) createNetworkListener() (net.Listener, error) {
	var list net.Listener
	var err error
	var tlsConfig *tls.Config
	listenAddress := s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID]
	if s.cfg.KafkaServerListenerConfig.TLSConfig.Enabled {
		tlsConfig, err = conf.CreateServerTLSConfig(s.cfg.KafkaServerListenerConfig.TLSConfig)
		if err != nil {
			return nil, errwrap.WithStack(err)
		}
		list, err = common.Listen("tcp", listenAddress)
		if err == nil {
			list = tls.NewListener(list, tlsConfig)
		}
	} else {
		list, err = common.Listen("tcp", listenAddress)
	}
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	return list, nil
}

func (s *Server) acceptLoop() {
	defer s.acceptLoopExitGroup.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Ok - was closed
			break
		}
		c := s.newConnection(conn)
		s.connections.Store(c, struct{}{})
		c.start()
	}
}

func (s *Server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			// Ignore
		}
	}
	// Wait for accept loop to exit
	s.acceptLoopExitGroup.Wait()

	// Now close connections
	s.connections.Range(func(conn, _ interface{}) bool {
		conn.(*connection).stop()
		return true
	})
	s.fetcher.stop()
	s.started = false
	return nil
}

func (s *Server) ListenAddress() string {
	return s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID]
}

func (s *Server) Connections() []*connection {
	var conns []*connection
	s.connections.Range(func(c, _ any) bool {
		conns = append(conns, c.(*connection))
		return true
	})
	return conns
}

func (s *Server) removeConnection(conn *connection) {
	s.connections.Delete(conn)
}

func (s *Server) newConnection(conn net.Conn) *connection {
	return &connection{
		s:    s,
		conn: conn,
	}
}

type connection struct {
	s                *Server
	conn             net.Conn
	closeGroup       sync.WaitGroup
	lock             sync.Mutex
	closed           bool
	authContext      auth.Context
	saslConversation auth.SaslConversation
}

func (c *connection) start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closeGroup.Add(1)
	common.Go(c.readLoop)
}

func (c *connection) readLoop() {
	defer c.readPanicHandler()
	defer c.closeGroup.Done()
	if err := c.readMessage(); err != nil {
		log.Errorf("error in reading from kafka connection: %v", err)
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
	}
	c.cleanUp()
}

func (c *connection) cleanUp() {
	c.s.removeConnection(c)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true
}

func (c *connection) readPanicHandler() {
	// We use a custom panic handler as we don't want the server to panic and crash if it receives a malformed
	// request which has insufficient bytes in the buffer which would cause a runtime error: index out of range panic
	if r := recover(); r != nil {
		log.Errorf("failure in connection readLoop: %v", r)
		if err := c.conn.Close(); err != nil {
			// Ignore
		}
		c.cleanUp()
	}
}

func (c *connection) readMessage() error {
	buff := make([]byte, readBuffSize)
	var err error
	var readPos, n int
	for {
		// read the message size
		bytesRequired := 4 - readPos
		if bytesRequired > 0 {
			n, err = io.ReadAtLeast(c.conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}

		totSize := 4 + int(binary.BigEndian.Uint32(buff))
		bytesRequired = totSize - readPos
		if bytesRequired > 0 {
			// If we haven't already read enough bytes, read the entire message body
			if totSize > len(buff) {
				// buffer isn't big enough, resize it
				nb := make([]byte, totSize)
				copy(nb, buff)
				buff = nb
			}
			n, err = io.ReadAtLeast(c.conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}
		// Note that the buffer is reused so it's up to the protocol structs to copy any data in the message such
		// as records, uuid, []byte before the call to handleMessage returns
		err = c.handleMessage(buff[4:totSize])
		if err != nil {
			break
		}

		remainingBytes := readPos - totSize
		if remainingBytes > 0 {
			// Bytes for another message(s) have already been read, don't throw these away
			if remainingBytes < totSize {
				// we can copy directly as no overlap
				copy(buff, buff[totSize:readPos])
			} else {
				// too many bytes remaining, we have to create a new buffer
				nb := make([]byte, len(buff))
				copy(nb, buff[totSize:readPos])
				buff = nb
			}
		}
		readPos = remainingBytes
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (c *connection) handleMessage(message []byte) error {
	if !c.authContext.Authenticated && c.s.cfg.KafkaServerListenerConfig.AuthenticationType == auth.AuthenticationTLS {
		if err := c.authoriseWithClientCert(); err != nil {
			return err
		}
	}
	apiKey := int16(binary.BigEndian.Uint16(message))
	authType := c.s.cfg.KafkaServerListenerConfig.AuthenticationType
	authenticated := authType == "" || apiKey == protocol.APIKeyAPIVersions || apiKey == protocol.APIKeySaslHandshake || apiKey == protocol.APIKeySaslAuthenticate || c.authContext.Authenticated
	if !authenticated {
		return errors.Errorf("cannot handle Kafka apiKey: %d as authentication type is %s but connection has not been authenticated", apiKey, authType)
	}
	return protocol.HandleRequestBuffer(apiKey, message, c, c.conn)
}

func (c *connection) authoriseWithClientCert() error {
	tlsConn, ok := c.conn.(*tls.Conn)
	if !ok {
		return errors.New("cannot use TLS authentication on Kafka connection - connection is not TLS")
	}
	pcs := tlsConn.ConnectionState().PeerCertificates
	if len(pcs) == 0 {
		return errors.New("cannot use TLS authentication on Kafka connection - TLS is not configured for client certificates")
	}
	if len(pcs) > 1 {
		return errors.New("client has provided more than one certificate - please make sure only one cerftificate is provided")
	}
	principal := pcs[0].Subject.String()
	c.lock.Lock()
	defer c.lock.Unlock()
	c.authContext.Principal = &principal
	c.authContext.Authenticated = true
	return nil
}

func (c *connection) stop() {
	c.lock.Lock()
	c.closed = true
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from Client)
	}
	c.lock.Unlock()
	c.closeGroup.Wait()
}

func (c *connection) AuthContext() auth.Context {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.authContext
}

type MetadataProvider interface {
	ControllerNodeID() int

	BrokerInfos() []BrokerInfo

	GetTopicInfo(topicName string) (TopicInfo, bool)

	GetAllTopics() []*TopicInfo
}

type BrokerInfo struct {
	NodeID int
	Host   string
	Port   int
}

type TopicInfo struct {
	Name                 string
	ProduceEnabled       bool
	ConsumeEnabled       bool
	CanCache             bool
	ProduceInfoProvider  TopicInfoProvider
	ConsumerInfoProvider ConsumerInfoProvider
	Partitions           []PartitionInfo
}

type TopicInfoProvider interface {
	PartitionScheme() *opers.PartitionScheme
	ReceiverID() int
	GetLastProducedInfo(partitionID int) (int64, int64)
	IngestBatch(recordBatchBytes []byte, processor proc.Processor, partitionID int,
		complFunc func(err error))
}

type ConsumerInfoProvider interface {
	SlabID() int
	PartitionScheme() *opers.PartitionScheme
	EarliestOffset(partitionID int) (int64, int64, bool)
	LatestOffset(partitionID int) (int64, int64, bool, error)
	OffsetByTimestamp(timestamp types.Timestamp, partitionID int) (int64, int64, bool)
}

type PartitionInfo struct {
	ID             int
	LeaderNodeID   int
	ReplicaNodeIDs []int
}
