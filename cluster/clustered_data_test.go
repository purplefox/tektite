package cluster

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

/*

 */

func TestClusteredData(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	td1 := createTestClusteredData(objStore)

	err := td1.clusteredData.TakeOwnership()
	require.NoError(t, err)

	require.Equal(t, "initial\n", string(td1.GetState()))
	require.True(t, td1.IsGetInitialStateCalled())
	require.False(t, td1.IsLoadedCalled())

	td1.AddToState("line1\n")
	ok, err := td1.Store()
	require.NoError(t, err)
	require.True(t, ok)

	td2 := createTestClusteredData(objStore)

	err = td2.clusteredData.TakeOwnership()
	require.NoError(t, err)

	require.False(t, td2.IsGetInitialStateCalled())
	require.True(t, td2.IsLoadedCalled())
	require.Equal(t, "initial\nline1\n", string(td2.GetState()))

	td2.AddToState("line2\n")
	ok, err = td2.Store()
	require.NoError(t, err)
	require.True(t, ok)

	td1.AddToState("line1.1\n")
	ok, err = td1.Store()
	require.NoError(t, err)
	// must fail as td2 is owner
	require.False(t, ok)

	td3 := createTestClusteredData(objStore)

	err = td3.clusteredData.TakeOwnership()
	require.NoError(t, err)

	// must be no line1.1
	require.False(t, td3.IsGetInitialStateCalled())
	require.True(t, td3.IsLoadedCalled())
	require.Equal(t, "initial\nline1\nline2\n", string(td2.GetState()))

}

func TestClusteredDataConcurrency(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	concurrency := 10
	updatesPerRunner := 100

	for i := 0; i < concurrency; i++ {

	}
}

type runner struct {
	objStore objstore.Client
	numUpdates int
}

func (r *runner) Run() error {
	for i := 0; i < r.numUpdates; i++ {
		td := createTestClusteredData(r.objStore)
		err := td.clusteredData.TakeOwnership()
		if err != nil {
			return err
		}

	}
}

func createTestClusteredData(objStore objstore.Client) *testData {
	td := &testData{}
	clusteredData := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, td, ClusteredDataOpts{})
	td.clusteredData = clusteredData
	td.data = []byte("initial\n")
	return td
}

type testData struct {
	lock                  sync.Mutex
	data                  []byte
	clusteredData         *ClusteredData
	getInitialStateCalled bool
	loadedCalled          bool
}

func (t *testData) Loaded(data []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.loadedCalled = true
	t.data = data
}

func (t *testData) GetInitialState() []byte {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.getInitialStateCalled = true
	return t.data
}

func (t *testData) IsGetInitialStateCalled() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.getInitialStateCalled
}

func (t *testData) IsLoadedCalled() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.loadedCalled
}

func (t *testData) Store() (bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.clusteredData.StoreData(t.data)
}

func (t *testData) AddToState(line string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.data = append(t.data, line...)
}

func (t *testData) GetState() []byte {
	t.lock.Lock()
	defer t.lock.Unlock()
	return common.ByteSliceCopy(t.data)
}
