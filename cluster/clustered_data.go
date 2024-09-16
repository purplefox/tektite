package cluster

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

/*
ClusteredData

We will create a new clustered state machine for storing controller metadata. We won't use the membership state machine
because this state machine will be updated more frequently and, in normal operation, only by the leader. The membership
state machine is updated by all members of the cluster and they would have to keep up with a lot of updates.

We do not currently replicate the controller state before pushing to the object store therefore we will push the state
to S3 before acknowledging any metadata update. If the caller receives a positive ack, they know the state was
successfully stored in object storage.

The state to be pushed consists of the serialized master record. We do not store this directly in the clustered state
machine, as the current S3 conditional write mechanism requires us to create a new key for each state machine state
update. The master record can be multiple megabytes in size and there can be many updates per second under high load.
Storing this many keys becomes problematic as it can create significant storage costs, even with, say, a modest bucket
expiration policy of 1 day.

Instead, we write the state to a key which includes an 'instance id' which is a unique id for the controller (UUID) that
is generated each time a controller becomes leader. This key is overwritten each time by the same leader. Different
leaders will mever overwrite each other's data keys. The key has the form `tektite-controller-state-<instance-id>`.

The value written in the controller clustered state machine will include this instance id, thus keeping the size of the
object small.

When a leader stores metadata, it first writes the data to the key outside the state machine, and then attempts to
update the clustered state machine with the current instance-id. This update is atomic and conditional and only succeeds
if the previous value of the instance-id in the state machine matches that expected by the current leader. Thus in
normal operation where the leader hasn't changed this succeeds.

This provides zombie fencing. Consider a cluster [A, B, C] where A is the leader. A is removed from the cluster
membership and B becomes leader, and B updates the controller state machine with its instance id. A (the zombie) is
still active and attempts to write metadata - it writes the data key then attempts to update the state machine but this
fails as it's instance id does not match that in the latest state machine.

When a replica becomes leader, it loads the actual master record data given the instance-id in the state machine.

As the data key is stored before the state machine is updated it could be the case a leader wrote the data key but
crashed before updating the state machine. Does this matter?

Consider [A, B, C] with A leader. A writes data then crashes before updating state machine. B becomes leader and
reads the instance-id from the state machine, it then reads the data from the key with that instance id - that
corresponds to data that was written without a state machine update. But it does not matter as it does not allow zombie
data to be read.

In this case we can accept the data even though the state machine hadn't been updated. In distributed systems, it's a
general principle that if an RPC to store some state fails to send back an acknowledgement to the caller, it does not
mean the state was not stored. The state could have been stored, then failure occurred before the ack was delivered back
to the caller. The caller can then decide to retry the operation. This is OK. Duplicate detection mechanisms can be
layered on top to help prevent duplicate retries being accepted, e.g. Kafka has idempotent producers that help with this.

An aside on parallel writes of data key and updating state machine:

As an optimisation in order to reduce overall latency, we can consider writing the data key and the state machine update
in parallel. Can we maintain correctness while doing this?

Let's consider the possible failure modes:

1. Data key is written, but leader crashes before updating state machine. In this case state without corresponding state
machine update can be read, but this does not allow zombies (see discussion above about this).
2. State machine is updated, but corresponding data key is not written. New leader loads state machine state and then
attempts to load data key for that instance id. It's possible the right data key hasn't been written yet (or will never
get written). The new leader then retries for up to 2 * timeout (let's call it data timeout), if it doesn't see the key then
it laods the latest key from all of the written data keys. If it does see the key within timeout, then great, it uses that.
On the other side, the old leader retries trying to write the data key for up timeout, if the leader succeeds in writing
but it took more than timeout then it returns failure to the caller. We would also need to write cluster version into the
state and the data object so the reader can validate whether it's loaded the correct data object for the data key.
*/
type ClusteredData struct {
	lock           sync.Mutex
	dataBucketName string
	dataKeyPrefix  string
	stateMachine   *StateMachine[string]
	objStoreClient objstore.Client
	instanceID     string
	dataCallback   DataCallback
	opts           ClusteredDataOpts
	loadStateTimer *time.Timer
	state          clusteredDataState
}

type DataCallback interface {
	Loaded(buff []byte)
	GetInitialState() []byte
}

func NewClusteredData(stateMachineBucketName string, stateMachineKeyPrefix string, dataBucketName string,
	dataKeyPrefix string, objStoreClient objstore.Client, metadata DataCallback, opts ClusteredDataOpts) *ClusteredData {
	return &ClusteredData{
		objStoreClient: objStoreClient,
		dataBucketName: dataBucketName,
		dataKeyPrefix:  dataKeyPrefix,
		instanceID:     uuid.New().String(),
		stateMachine: NewStateMachine[string](stateMachineBucketName, stateMachineKeyPrefix, objStoreClient,
			StateMachineOpts{}),
		opts:         opts,
		dataCallback: metadata,
	}
}

const DefaultLoadStateRetryInterval = 5 * time.Second

type clusteredDataState int

const (
	stateNotOwner clusteredDataState = iota
	stateLoading
	stateOwner
	stateFailedToTakeOwnership
)

type ClusteredDataOpts struct {
	LoadStateRetryInterval time.Duration
}

func (mo *ClusteredDataOpts) setDefaults() {
	if mo.LoadStateRetryInterval == 0 {
		mo.LoadStateRetryInterval = DefaultLoadStateRetryInterval
	}
}

func (m *ClusteredData) TakeOwnership() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.state != stateNotOwner {
		return errors.Errorf("cannot take ownership, state is %d", m.state)
	}
	m.state = stateLoading
	return m.loadData()
}

func (m *ClusteredData) StoreData(data []byte) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.state != stateOwner {
		return false, errwrap.New("cannot store data, state is not owner")
	}
	return m.storeData(m.instanceID, data)
}

next: make this synchrinous and retry on unavailable 
func (m *ClusteredData) loadData() error {
	ok, err := m.doLoadData()
	if err == nil {
		if ok {
			m.state = stateOwner
		} else {
			// Failed to store initial state as another leaded sneaked in
			log.Infof("failed to load state as another node became leader")
			m.state = stateFailedToTakeOwnership
		}
		return nil
	}
	if common.IsUnavailableError(err) {
		// schedule another attempt
		m.loadStateTimer = time.AfterFunc(m.opts.LoadStateRetryInterval, func() {
			m.lock.Lock()
			defer m.lock.Unlock()
			if m.state != stateLoading {
				return
			}
			if err := m.loadData(); err != nil {
				log.Warnf("Error loading controller state: %v", err)
			}
		})
		return nil
	}
	return err
}

func (m *ClusteredData) createDataKey(instanceID string) string {
	return fmt.Sprintf("%s-%s", m.dataKeyPrefix, instanceID)
}

func (m *ClusteredData) doLoadData() (bool, error) {
	// Update with a no-op to load latest data
	lastInstanceID, err := m.stateMachine.Update(func(state string) (string, error) {
		return state, nil
	})
	if err != nil {
		return false, err
	}
	var data []byte
	if lastInstanceID != "" {
		dataKey := m.createDataKey(lastInstanceID)
		// TODO retries and timeout
		data, err = m.objStoreClient.Get(context.Background(), m.dataBucketName, dataKey)
		if err != nil {
			return false, err
		}
		if len(data) == 0 {
			// Metadata state is missing. We always store the state before updating the state machine, so this should never
			// occur
			return false, errors.Errorf("metadata key '%s' not found - cannot start controller", dataKey)
		}
	}
	m.instanceID = uuid.New().String()

	// Store the metadata and update ourself in the state machine
	var dataToStore []byte
	if data != nil {
		dataToStore = data
	} else {
		dataToStore = m.dataCallback.GetInitialState()
		if len(dataToStore) == 0 {
			return false, errors.New("initial state cannot be nil or empty")
		}
	}
	ok, err := m.storeData(lastInstanceID, dataToStore)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if data != nil {
		m.dataCallback.Loaded(data)
	}
	return true, nil
}

func (m *ClusteredData) storeData(prevInstanceID string, metaData []byte) (bool, error) {
	// First we store the actual data under a key that includes the instance id
	metadataKey := m.createDataKey(m.instanceID)
	// TODO retries and timeout
	if err := m.objStoreClient.Put(context.Background(), m.dataBucketName, metadataKey, metaData); err != nil {
		return false, err
	}
	// Then we conditionally update the state machine with our instance id only if previous instance id is what we
	// expect
	newInstanceID, err := m.stateMachine.Update(func(instanceID string) (string, error) {
		if prevInstanceID != instanceID {
			return instanceID, nil
		}
		// Update with our instance id
		return m.instanceID, nil
	})
	if err != nil {
		return false, err
	}
	if newInstanceID != m.instanceID {
		// Conditional update didn't succeed - another controller has updated and become leader, we must stop
		log.Infof("controller %s failed to store metadata as no longer leader", m.instanceID)
		return false, nil
	}
	return true, nil
}
