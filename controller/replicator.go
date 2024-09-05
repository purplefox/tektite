package controller

import (
	"encoding/binary"
	"github.com/graph-gophers/graphql-go/errors"
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

type Replicator struct {
	replicationFactor int
	minReplications int
}

type ReplicationGroup struct {
	replicator *Replicator
	lock                  sync.Mutex
	transport             Transport
	connections          map[string]Connection
	clusterStateProvider ClusterState
	id                   int
	commandSequence       int64
	lastFlushedCommandSeq int64
	epoch                 int64
	commandType           CommandType
	responseChan          atomic.Pointer[chan replicationResponse]
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
	case ErrorCodeNode: return ""
	case ErrorCodeInvalidEpoch: return "replica invalid epoch"
	case ErrorCodeNeedsInitialState: return "replica needs initial state"
	case ErrorCodeInternalError: return "replica internal error"
	default:
		panic("unknown replicator code")
	}
}

func (r *ReplicationGroup) getConnection(address string) (Connection, error) {
	conn, exists := r.connections[address]
	if exists {
		return conn, nil
	}
	conn, err := r.transport.CreateConnection(address, func(message []byte) error {
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
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.commandSequence))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.lastFlushedCommandSeq))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.epoch))
	buff = binary.BigEndian.AppendUint16(buff, uint16(r.commandType))
	buff, err := command.Serialize(buff)
	if err != nil {
		return err
	}
	r.commandSequence++
	followers := r.clusterStateProvider.FollowerAddresses()
	respCh := make(chan replicationResponse, len(followers))
	r.responseChan.Store(&respCh)
	for _, follower := range followers {
		conn, err := r.getConnection(follower)
		if err != nil {
			return err
		}
		if err := conn.WriteMessage(buff); err != nil {
			r.clusterStateProvider.FollowerFailure(follower, err)
			return err
		}
	}
	successes := 0
	for replResp := range respCh {
		if replResp.errorCode != ErrorCodeNode {
			err := errors.Errorf("Replica %s returned error code %d %s", replResp.address, replResp.errorCode,
				replResp.errorCode.String())
			r.clusterStateProvider.FollowerFailure(replResp.address, err)
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

func (r *ReplicationGroup) Close() {
	for _, conn := range r.connections {
		if err := conn.Close(); err != nil {
			log.Debugf("failed to close replication group connection: %v", err)
		}
	}
}
