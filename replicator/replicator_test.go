package replicator

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

/*

* create a simple handler (maybe rename to statemachine) and apply commands to it, at end of test verify that state on all replicas is the same
* now simulate transient network failure such that it is retried and works, and followers get duplicates, make sure they are rejected and state machines are equal.
* test with network retries past limit and replica is removed from group
* test with replica returning wrong epoch - no need to remove from group here as already wrong epoch.
* test failover to another up to date replica - what if new leader has higher state than followers? this can happen if previous replication failed half way. think about whether this matters?
* test bouncing follower, make sure it re-initialises ok
* test with flushing.
* chaos test - set up a cluster and inject randomly: 1) network errors, then add/remove nodes randomly. verify state is always the same at points, and at end of test.

 */

func TestReplicator(t *testing.T) {
	numReplicas := 3

	localTransports := transport.NewLocalTransports()

	replicators := make([]*Replicator, numReplicas)

	numGroups := 10

	initialState := cluster.MembershipState{
		Epoch: 1,
	}

	for i := 0; i < numReplicas; i++ {
		address := uuid.New().String()
		initialState.Members = append(initialState.Members, cluster.MembershipEntry{Address: address})
		trans, err := localTransports.NewLocalTransport(address)
		require.NoError(t, err)
		replicator := NewReplicator(trans, numReplicas, address, func(s string) {
		})
		replicator.RegisterStateMachineFactory(testCommandType, func() StateMachine {
			return &testCommandHandler{}
		}, numGroups)
		replicators[i] = replicator
	}

	command := &testCommand{
		data: "some-data",
	}

	// Apply the initial cluster state
	for i := 0; i < numReplicas; i++ {
		replicators[i].MembershipChanged(initialState)
	}

	_, err := replicators[0].ApplyCommand(command)

	for i := 0; i < numReplicas; i++ {
		groups := replicators[i].GetGroups(testCommandType)
		require.Equal(t, numGroups, len(groups))
		group := groups[0]
		require.NotNil(t, group)
		handler := group.stateMachine.(*testCommandHandler)
		commands := handler.getCommands()
		require.Equal(t, 1, len(commands))
		require.Equal(t, command, commands[0])
	}

	require.NoError(t, err)
}

type testCommandHandler struct {
	lock     sync.Mutex
	commands []Command
}

func (t *testCommandHandler) HandleLeader(command Command) (any, error) {
	return nil, nil
}

func (t *testCommandHandler) getCommands() []Command {
	t.lock.Lock()
	defer t.lock.Unlock()
	commandsCopy := make([]Command, len(t.commands))
	copy(commandsCopy, t.commands)
	return commandsCopy
}

func (t *testCommandHandler) UpdateState(command Command, commandSequence int64, lastFlushedSequence int64) (any, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.commands = append(t.commands, command)
	return nil, nil
}

func (t *testCommandHandler) Flush(completionFunc func(lastFlushedSequence int64) error) error {
	return nil
}

func (t *testCommandHandler) NewCommand() (Command, error) {
	return &testCommand{}, nil
}

const testCommandType = CommandType(2)

type testCommand struct {
	data string
}

func (t *testCommand) Type() CommandType {
	return testCommandType
}

func (t *testCommand) GroupID(numGroups int) int64 {
	return 0
}

func (t *testCommand) Serialize(buff []byte) ([]byte, error) {
	ltd := len(t.data)
	buff = binary.BigEndian.AppendUint32(buff, uint32(ltd))
	buff = append(buff, t.data...)
	return buff, nil
}

func (t *testCommand) Deserialize(buff []byte) ([]byte, error) {
	ltd := binary.BigEndian.Uint32(buff)
	end := 4 + int(ltd)
	t.data = string(buff[4:end])
	return buff[end:], nil
}

type testClusterState struct {
	leader    int
	followers []string
}

func (t *testClusterState) FollowerAddresses() []string {
	return t.followers
}

func (t *testClusterState) FollowerFailure(address string, err error) {
	log.Errorf("follower %s failed: %v", address, err)
}
