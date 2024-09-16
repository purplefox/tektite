package controller

import (
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"sync"
	"time"
)

/*
Manager

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
type Manager struct {
	lock               sync.RWMutex
	metadataBucketName string
	opts               ManagerOpts
	readyState         readyState
	stateMachine       *cluster.StateMachine[string]
	objStoreClient     objstore.Client
	metadataKeyPrefix  string
	lsmMgr             *lsm.Manager
	loadStateTimer     *time.Timer
	instanceID         string
}

func NewManager(objStoreClient objstore.Client, stateMachineBucketName string, stateKeyPrefix string,
	metadataBucketName string, metadataKeyPrefix string, opts ManagerOpts) *Manager {
	opts.setDefaults()
	smOpts := cluster.StateMachineOpts{}
	sm := cluster.NewStateMachine[string](stateMachineBucketName, stateKeyPrefix, objStoreClient, smOpts)
	return &Manager{
		objStoreClient:     objStoreClient,
		metadataKeyPrefix:  metadataKeyPrefix,
		stateMachine:       sm,
		metadataBucketName: metadataBucketName,
		opts:               opts,
	}
}

type ManagerOpts struct {
	LoadStateRetryInterval time.Duration
}

func (mo *ManagerOpts) setDefaults() {
	if mo.LoadStateRetryInterval == 0 {
		mo.LoadStateRetryInterval = cluster.DefaultLoadStateRetryInterval
	}
}
