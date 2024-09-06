package replicator

import (
	"encoding/binary"
	"github.com/google/uuid"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

type testCommandHandler struct {
	lock     sync.Mutex
	commands []Command
}

func (t *testCommandHandler) NewCommand() (Command, error) {
	return &testCommand{}, nil
}

func (t *testCommandHandler) Handle(command Command) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.commands = append(t.commands, command)
	return nil
}

func TestReplicator(t *testing.T) {
	numReplicas := 3
	localTransports := transport.NewLocalTransports()

	replicators := make([]*Replicator, numReplicas)

	clusterState := &testClusterState{}

	testCommandType := CommandType(23)

	handlers := make([]*testCommandHandler, numReplicas)

	for i := 0; i < numReplicas; i++ {
		address := uuid.New().String()
		if i != 0 {
			clusterState.followers = append(clusterState.followers, address)
		}
		transport, err := localTransports.NewLocalTransport(address)
		require.NoError(t, err)
		replicator := NewReplicator(transport, numReplicas, clusterState)
		handlers[i] = &testCommandHandler{}
		replicator.RegisterCommandHandler(testCommandType, handlers[i])
		replicators[i] = replicator
	}

	group := replicators[0].NewReplicationGroup(testCommandType, nil)

	command := &testCommand{
		data: "some-data",
	}

	err := group.ReplicateCommand(command)
	require.NoError(t, err)
}

type testCommand struct {
	data string
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
