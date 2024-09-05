package controller

import (
	"encoding/binary"
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
)

type CommandPacket struct {
	GroupID               int64
	CommandSeq            int64
	LastFlushedCommandSeq int64
	Epoch                 int64
	CommandType           int16
}

type CommandResponse struct {
	ReplicatorID int
	CommandID    int
	Status       CommandStatus
}

type CommandStatus int

type CommandType int16

type Command interface {
	Serialize(buff []byte) ([]byte, error)
	Deserialize(buff []byte) ([]byte, error)
}

type HandlerFactories struct {
	factories map[CommandType]HandlerFactory
}

type HandlerFactory interface {
	CreateCommand(commandType CommandType) Command
}

type ClusterState interface {
	FollowerAddresses() []string
	FollowerFailure(address string, err error)
}

func NewReplicator(transport Transport, replicationFactor int, clusterState ClusterState) *Replicator {
	r := &Replicator{
		transport:         transport,
		replicationFactor: replicationFactor,
		minReplications:   (replicationFactor + 1) / 2,
		clusterState:      clusterState,
	}
	transport.RegisterHandler(r.handleRequest)
	return r
}

type Replicator struct {
	lock              sync.RWMutex
	transport         Transport
	replicationFactor int
	minReplications   int
	clusterState      ClusterState
	groups            map[int]*ReplicationGroup
}

func (r *Replicator) handleRequest(message []byte, responseWriter ResponseHandler) error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	groupID := binary.BigEndian.Uint64(message)
	group, ok := r.groups[int(groupID)]
	if !ok {
		return errors.Errorf("group %d not found", groupID)
	}
	return group.handleRequest(message[8:], responseWriter)
}

func (r *Replicator) NewReplicationGroup() *ReplicationGroup {
	return &ReplicationGroup{
		replicator:  r,
		connections: make(map[string]Connection),
	}
}

type ReplicationGroup struct {
	replicator                *Replicator
	lock                      sync.Mutex
	commandType               int
	connections               map[string]Connection
	id                        int
	lastSentCommandSeq        int64
	lastSentFlushedCommandSeq int64

	lastReceivedCommandSeq        int64
	lastReceivedFlushedCommandSeq int64
	epoch                         int64
	responseChan                  atomic.Pointer[chan replicationResponse]
}

type ErrorCode int16

const (
	ErrorCodeNode              ErrorCode = 0
	ErrorCodeInvalidEpoch      ErrorCode = 1
	ErrorCodeNeedsInitialState ErrorCode = 2
	ErrorCodeInternalError     ErrorCode = 3
)

func (e ErrorCode) String() string {
	switch e {
	case ErrorCodeNode:
		return ""
	case ErrorCodeInvalidEpoch:
		return "replica invalid epoch"
	case ErrorCodeNeedsInitialState:
		return "replica needs initial state"
	case ErrorCodeInternalError:
		return "replica internal error"
	default:
		panic("unknown replicator code")
	}
}

func (r *ReplicationGroup) getConnection(address string) (Connection, error) {
	conn, exists := r.connections[address]
	if exists {
		return conn, nil
	}
	conn, err := r.replicator.transport.CreateConnection(address, func(message []byte) error {
		return r.handleResponse(address, message)
	})
	if err != nil {
		return nil, err
	}
	r.connections[address] = conn
	return conn, nil
}

func (r *ReplicationGroup) handleResponse(address string, response []byte) error {
	responseCode := ErrorCode(binary.BigEndian.Uint16(response))
	ch := r.responseChan.Load()
	*ch <- replicationResponse{
		address:   address,
		errorCode: responseCode,
	}
	return nil
}

type replicationResponse struct {
	address   string
	errorCode ErrorCode
}

func (r *ReplicationGroup) ReplicateCommand(command Command) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	buff := make([]byte, 0, 64)
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.id))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.lastSentCommandSeq))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.lastSentFlushedCommandSeq))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.epoch))
	buff = binary.BigEndian.AppendUint16(buff, uint16(r.commandType))
	buff, err := command.Serialize(buff)
	if err != nil {
		return err
	}
	r.lastSentCommandSeq++
	followers := r.replicator.clusterState.FollowerAddresses()
	respCh := make(chan replicationResponse, len(followers))
	r.responseChan.Store(&respCh)
	for _, follower := range followers {
		conn, err := r.getConnection(follower)
		if err != nil {
			return err
		}
		if err := conn.WriteMessage(buff); err != nil {
			r.replicator.clusterState.FollowerFailure(follower, err)
			return err
		}
	}
	successes := 0
	for replResp := range respCh {
		if replResp.errorCode != ErrorCodeNode {
			err := errors.Errorf("Replica %s returned error code %d %s", replResp.address, replResp.errorCode,
				replResp.errorCode.String())
			r.replicator.clusterState.FollowerFailure(replResp.address, err)
		} else {
			successes++
		}
	}
	if successes < r.replicator.minReplications {
		// Insufficient replications
		return errors.Errorf("command not replicated to sufficient replicas, required %d actual %d",
			r.replicator.minReplications, successes)
	}
	return nil
}

func (r *ReplicationGroup) handleRequest(message []byte, responseWriter ResponseHandler) error {
	commandSequence := int64(binary.BigEndian.Uint64(message))
	lastFlushedCommandSeq := int64(binary.BigEndian.Uint64(message[8:]))

}

func (r *ReplicationGroup) Close() {
	for _, conn := range r.connections {
		if err := conn.Close(); err != nil {
			log.Debugf("failed to close replication group connection: %v", err)
		}
	}
}
