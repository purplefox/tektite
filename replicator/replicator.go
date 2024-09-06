package replicator

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/cluster"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
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
	Type() CommandType
	GroupID(numGroups int) int64
	Serialize(buff []byte) ([]byte, error)
	Deserialize(buff []byte) ([]byte, error)
}

func NewReplicator(transport transport.Transport, replicationFactor int, address string, memberFailedFunc func(string)) *Replicator {
	r := &Replicator{
		transport:         transport,
		address:           address,
		replicationFactor: replicationFactor,
		minReplications:   (replicationFactor + 1) / 2,
		memberFailedFunc:  memberFailedFunc,
	}
	transport.RegisterHandler(r.handleRequest)
	return r
}

type Replicator struct {
	lock              sync.RWMutex
	address           string
	transport         transport.Transport
	replicationFactor int
	minReplications   int
	handlerGroups     [][]*ReplicationGroup
	memberFailedFunc  func(string)
	membership        *cluster.MembershipState
	leader            bool
}

type StateMachineFactory func() StateMachine

type StateMachine interface {
	NewCommand() (Command, error)
	UpdateState(command Command, commandSequence int64, lastFlushedSequence int64) (any, error)
	// Flush is called periodically to signal the handler that it should flush it's state. When it has done so, the
	// handler must call `completionFunc` with the last flushed sequence
	Flush(completionFunc func(lastFlushedSequence int64) error) error
}

func (r *Replicator) RegisterStateMachineFactory(typ CommandType, factory StateMachineFactory, numGroups int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if len(r.handlerGroups) < int(typ)+1 {
		newGroups := make([][]*ReplicationGroup, int(typ)+1)
		copy(newGroups, r.handlerGroups)
		r.handlerGroups = newGroups
	}
	groups := r.handlerGroups[typ]
	if groups != nil {
		panic(errors.Errorf("Command factory already registered: %d", typ))
	}
	group := make([]*ReplicationGroup, numGroups)
	for i := 0; i < numGroups; i++ {
		group[i] = &ReplicationGroup{
			replicator:        r,
			id:                i,
			commandType:       typ,
			stateMachine:      factory(),
			connections:       make(map[string]transport.Connection),
			flushedCommandSeq: -1,
		}
	}
	r.handlerGroups[typ] = group
}

func (r *Replicator) ApplyCommand(command Command) (any, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.membership == nil || !r.leader {
		return nil, &ReplicationError{Msg: "not leader", ErrorCode: ErrorCodeNotLeader}
	}
	if len(r.membership.Members)-1 < r.minReplications {
		return nil, &ReplicationError{
			Msg:       "insufficient followers",
			ErrorCode: ErrorCodeInsufficientFollowers,
		}
	}
	handlerGroups := r.handlerGroups[command.Type()]
	groupID := command.GroupID(len(handlerGroups))
	group := handlerGroups[groupID]
	res, err := group.stateMachine.UpdateState(command, group.commandSeq, group.flushedCommandSeq)
	if err != nil {
		return nil, err
	}
	if err := group.replicateCommand(command); err != nil {
		return nil, err
	}
	return res, nil
}

func (r *Replicator) MembershipChanged(membership cluster.MembershipState) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.membership = &membership
	prevLeader := r.leader
	r.leader = len(r.membership.Members) > 0 && r.membership.Members[0].Address == r.address
	if !prevLeader && r.leader {
		r.promoteToLeader()
	} else if prevLeader && !r.leader {
		r.demoteFromLeader()
	}
}

func (r *Replicator) promoteToLeader() {
	// TODO
	// call all groups to promote to leader
	// groups need to initialise state, and when they are done they will mark themselves as ready

	// start flush timer
}

func (r *Replicator) demoteFromLeader() {
	// TODO
	// stop flush timer
}

func (r *Replicator) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, groups := range r.handlerGroups {
		for _, group := range groups {
			group.Close()
		}
	}
	r.handlerGroups = nil
}

func (r *Replicator) GetGroups(commandType CommandType) map[int]*ReplicationGroup {
	r.lock.RLock()
	defer r.lock.RUnlock()
	groups := r.handlerGroups[commandType]
	groupsCopy := make(map[int]*ReplicationGroup, len(groups))
	for id, group := range groups {
		groupsCopy[id] = group
	}
	return groupsCopy
}

func (r *Replicator) handleRequest(message []byte, responseWriter transport.ResponseHandler) error {
	r.lock.RLock()
	defer r.lock.RUnlock()
	commandType := CommandType(binary.BigEndian.Uint16(message))
	groupID := int(binary.BigEndian.Uint64(message[2:]))
	group := r.handlerGroups[commandType][groupID]
	return group.handleRequest(message[10:], responseWriter)
}

type ReplicationGroup struct {
	replicator        *Replicator
	lock              sync.Mutex
	commandType       CommandType
	connections       map[string]transport.Connection
	id                int
	stateMachine      StateMachine
	commandSeq        int64
	flushedCommandSeq int64
	epoch             int64
	responseHolder    atomic.Pointer[responseChanHolder]
}

type ErrorCode int16

const (
	ErrorCodeNode                     ErrorCode = 0
	ErrorCodeInvalidEpoch             ErrorCode = 1
	ErrorCodeNeedsInitialState        ErrorCode = 2
	ErrorCodeInternalError            ErrorCode = 3
	ErrorCodeNotLeader                ErrorCode = 4
	ErrorCodeInsufficientFollowers    ErrorCode = 5
	ErrorCodeInsufficientReplications ErrorCode = 6
)

type ReplicationError struct {
	Msg       string
	ErrorCode ErrorCode
}

func (r ReplicationError) Error() string {
	return fmt.Sprintf("replication error: %d %s", r.ErrorCode, r.Msg)
}

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

func (r *ReplicationGroup) getConnection(address string) (transport.Connection, error) {
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
	commandSequence := int64(binary.BigEndian.Uint64(response))
	responseCode := ErrorCode(binary.BigEndian.Uint16(response[8:]))
	holder := *r.responseHolder.Load()
	// FIXME we should pass back epoch too just to be on safe side
	if holder.commandSequence != commandSequence {
		log.Warnf("Unexpected command sequence in response %d expected %d", commandSequence, holder.commandSequence)
		return nil
	}
	holder.ch <- replicationResponse{
		address:   address,
		errorCode: responseCode,
	}
	return nil
}

type responseChanHolder struct {
	commandSequence int64
	ch              chan replicationResponse
}

type replicationResponse struct {
	address   string
	errorCode ErrorCode
}

const (
	replicateTimeout             = 5 * time.Second
	networkErrorMaxRetryDuration = 10 * time.Second
	networkErrorRetryInterval    = 1 * time.Second
)

func (r *ReplicationGroup) replicateCommand(command Command) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	buff := make([]byte, 0, 64)
	buff = binary.BigEndian.AppendUint16(buff, uint16(r.commandType))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.id))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.commandSeq))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.flushedCommandSeq))
	buff = binary.BigEndian.AppendUint64(buff, uint64(r.epoch))
	buff, err := command.Serialize(buff)
	if err != nil {
		return err
	}
	members := r.replicator.membership.Members
	followers := members[1:]
	respHolder := &responseChanHolder{
		commandSequence: r.commandSeq,
		ch:              make(chan replicationResponse, len(followers)),
	}
	r.commandSeq++
	r.responseHolder.Store(respHolder)
	sentFollowers := make([]string, 0, len(followers))
	for _, follower := range followers {
		address := follower.Address
		conn, err := r.getConnection(address)
		if err != nil {
			return err
		}
		sent := r.sendReplicationWithRetry(conn, address, buff)
		if sent {
			sentFollowers = append(sentFollowers, address)
		}
	}
	if len(sentFollowers) < r.replicator.minReplications {
		return &ReplicationError{
			Msg:       "failed to replicate to sufficient followers",
			ErrorCode: ErrorCodeInsufficientReplications,
		}
	}
	// TODO should we check received addresses???
	var successes int
loop:
	for i := 0; i < len(sentFollowers); i++ {
		select {
		case resp := <-respHolder.ch:
			if resp.errorCode != ErrorCodeNode {
				log.Warnf("Replica %s returned error code %d %s", resp.address, resp.errorCode,
					resp.errorCode.String())
				r.replicator.memberFailedFunc(resp.address)
			} else {
				successes++
			}
		case <-time.After(replicateTimeout):
			break loop
		}
	}
	if successes < r.replicator.minReplications {
		// Insufficient replications
		return errors.Errorf("unable to replicate to sufficient replicas")
	}
	return nil
}

func (r *ReplicationGroup) sendReplicationWithRetry(conn transport.Connection, address string, message []byte) bool {
	start := arista.NanoTime()
	for {
		err := conn.WriteMessage(message)
		if err == nil {
			return true
		}
		if int64(arista.NanoTime()-start) >= networkErrorMaxRetryDuration.Nanoseconds() {
			log.Errorf("error in writing replication, follower will be marked invalid %v", err)
			r.replicator.memberFailedFunc(address)
			return false
		}
		log.Warnf("error when sending replication - will retry %v", err)
		time.Sleep(networkErrorRetryInterval)
		continue
	}
}

func (r *ReplicationGroup) handleRequest(message []byte, responseWriter transport.ResponseHandler) error {
	commandSeq := int64(binary.BigEndian.Uint64(message))
	flushedCommandSeq := int64(binary.BigEndian.Uint64(message[8:]))
	epoch := int64(binary.BigEndian.Uint64(message[16:]))

	// TODO check not leader

	if commandSeq < r.commandSeq {
		log.Warnf("replicator group %d duplicate command received: %d expected: %d", r.id, commandSeq,
			r.commandSeq+1)
		return r.writeReplicationResponse(ErrorCodeNode, commandSeq, responseWriter)
	}
	if commandSeq > r.commandSeq+1 {
		log.Warnf("replicator group %d unexpected command received: %d expected: %d - needs initialisation", r.id, commandSeq,
			r.commandSeq+1)
		return r.writeReplicationResponse(ErrorCodeNeedsInitialState, commandSeq, responseWriter)
	}
	if r.epoch != epoch {
		log.Warnf("replicator group %d received replication at wrong epoch received: %d expected: %d", r.id, epoch,
			r.epoch)
		return r.writeReplicationResponse(ErrorCodeInvalidEpoch, commandSeq, responseWriter)
	}
	command, err := r.createCommand(message[24:])
	if err != nil {
		return err
	}
	_, err = r.stateMachine.UpdateState(command, commandSeq, flushedCommandSeq)
	if err != nil {
		return err
	}
	return r.writeReplicationResponse(ErrorCodeNode, commandSeq, responseWriter)
}

func (r *ReplicationGroup) createCommand(message []byte) (Command, error) {
	command, err := r.stateMachine.NewCommand()
	if err != nil {
		return nil, err
	}
	_, err = command.Deserialize(message)
	if err != nil {
		return nil, err
	}
	return command, nil
}

func (r *ReplicationGroup) writeReplicationResponse(errorcode ErrorCode, commandSequence int64, responseWriter transport.ResponseHandler) error {
	buff := make([]byte, 20)
	binary.BigEndian.PutUint64(buff, uint64(r.id))
	binary.BigEndian.PutUint64(buff[8:], uint64(commandSequence))
	binary.BigEndian.PutUint16(buff[16:], uint16(errorcode))
	return responseWriter(buff)
}

func (r *ReplicationGroup) Close() {
	for _, conn := range r.connections {
		if err := conn.Close(); err != nil {
			log.Debugf("failed to close replication group connection: %v", err)
		}
	}
}
