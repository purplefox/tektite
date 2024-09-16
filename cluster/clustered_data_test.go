package cluster

import (
	"encoding/binary"
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func addLineToData(data *testData, line string) {
	newData := append(data.GetData(), []byte(line+"\n")...)
	data.SetData(newData)
}

func TestClusteredData(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	td1 := createTestClusteredData(objStore)
	td1.SetInitialData([]byte("initial\n"))

	ok, err := td1.clusteredData.TakeOwnership()
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, "initial\n", string(td1.GetData()))
	require.True(t, td1.IsLoadedCalled())

	addLineToData(td1, "line1")
	ok, err = td1.Store()
	require.NoError(t, err)
	require.True(t, ok)

	td2 := createTestClusteredData(objStore)

	ok, err = td2.clusteredData.TakeOwnership()
	require.NoError(t, err)
	require.True(t, ok)

	require.True(t, td2.IsLoadedCalled())
	require.Equal(t, "initial\nline1\n", string(td2.GetData()))

	addLineToData(td2, "line2")
	ok, err = td2.Store()
	require.NoError(t, err)
	require.True(t, ok)

	addLineToData(td1, "line1.1")
	ok, err = td1.Store()
	require.NoError(t, err)
	// must fail as td2 is owner
	require.False(t, ok)

	td3 := createTestClusteredData(objStore)

	ok, err = td3.clusteredData.TakeOwnership()
	require.NoError(t, err)

	// must be no line1.1
	require.True(t, td3.IsLoadedCalled())
	require.Equal(t, "initial\nline1\nline2\n", string(td2.GetData()))
}

func TestInLoop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		log.Infof("iteration %d", i)
		TestClusteredDataConcurrency(t)
	}
}

func TestClusteredDataConcurrency(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	concurrency := 10
	updatesPerRunner := 100

	var runners []runner
	for i := 0; i < concurrency; i++ {
		runners = append(runners, runner{
			objStore:    objStore,
			runnerIndex: i,
			numUpdates:  updatesPerRunner,
			completeCh:  make(chan error, 1),
		})
	}
	for _, runner := range runners {
		r := runner
		go func() {
			r.Run()
		}()
	}
	for _, runner := range runners {
		err := <-runner.completeCh
		require.NoError(t, err)
	}
	// Check final state
	td := createTestClusteredData(objStore)
	ok, err := td.clusteredData.TakeOwnership()
	require.NoError(t, err)
	require.True(t, ok)

	m := deserializeMap(td.GetData())
	//log.Infof("final state: %v", m)
	require.Equal(t, concurrency, len(m))
	for i := 0; i < concurrency; i++ {
		// Note, this is greater or equal as we can get duplicates - this is OK
		require.GreaterOrEqual(t, m[i], updatesPerRunner)
	}
}

type runner struct {
	objStore    objstore.Client
	runnerIndex int
	numUpdates  int
	completeCh  chan error
}

func (r *runner) Run() {
	r.completeCh <- r.run()
}

func (r *runner) run() error {
	for i := 0; i < r.numUpdates; i++ {
	retry:
		for {
			td := createTestClusteredData(r.objStore)
			td.SetInitialData(make([]byte, 8)) // map with zero entries
			//log.Infof("%s runner %d taking ownership- update %d", td.clusteredData.instanceID, r.runnerIndex, i+1)
			ok, err := td.clusteredData.TakeOwnership()
			if err != nil {
				return err
			}
			//log.Infof("%s runner %d taking ownership- update %d - ok? %t", td.clusteredData.instanceID, r.runnerIndex, i+1, ok)

			if !ok {
				time.Sleep(1 * time.Millisecond)
				continue retry
			}
			data := td.GetData()
			// deserialize map
			m := deserializeMap(data)

			prev := m[r.runnerIndex]
			if i > 0 {
				if prev < i {
					// lost data
					return errors.Errorf("runner %d expected at least previous data %d found %d", r.runnerIndex, i, prev)
				}
			}

			// update map
			m[r.runnerIndex] = i + 1
			// serialize map
			buff := serializeMap(m)
			td.SetData(buff)
			//log.Infof("%s runner %d storing update %d", td.clusteredData.instanceID, r.runnerIndex, i+1)
			ok, err = td.Store()
			if err != nil {
				return err
			}
			//log.Infof("%s runner %d storing update %d - ok? %t", td.clusteredData.instanceID, r.runnerIndex, i+1, ok)
			if ok {
				break
			}
		}
	}
	return nil
}

func deserializeMap(data []byte) map[int]int {
	numEntries := int(binary.BigEndian.Uint64(data))
	offset := 8
	m := make(map[int]int, numEntries)
	for i := 0; i < numEntries; i++ {
		k := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		v := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		m[k] = v
	}
	return m
}

func serializeMap(m map[int]int) []byte {
	buff := binary.BigEndian.AppendUint64(nil, uint64(len(m)))
	for k, v := range m {
		buff = binary.BigEndian.AppendUint64(buff, uint64(k))
		buff = binary.BigEndian.AppendUint64(buff, uint64(v))
	}
	return buff
}

func createTestClusteredData(objStore objstore.Client) *testData {
	td := &testData{}
	clusteredData := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, td, ClusteredDataOpts{})
	td.clusteredData = clusteredData
	return td
}

type testData struct {
	lock          sync.Mutex
	data          []byte
	clusteredData *ClusteredData
	loadedCalled  bool
	initialData   []byte
}

func (t *testData) Loaded(data []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.loadedCalled = true
	if data == nil {
		t.data = t.initialData
	} else {
		t.data = data
	}
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

func (t *testData) GetData() []byte {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.data
}

func (t *testData) SetInitialData(data []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.initialData = data
}

func (t *testData) SetData(data []byte) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.data = data
}
