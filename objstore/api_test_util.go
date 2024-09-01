package objstore

import (
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestApi(t *testing.T, client Client) {

	testCases := []testCase{
		{testName: "testPutGet", test: testPutGet},
		{testName: "testPutOverwrite", test: testPutOverwrite},
		{testName: "testDelete", test: testDelete},
		{testName: "testDeleteAll", test: testDeleteAll},
		{testName: "testPutIfNotExists", test: testPutIfNotExists},
		{testName: "testListObjects", test: testListObjects},
		{testName: "testListObjectsNilPrefix", test: testListObjectsNilPrefix},
		{testName: "testListMoreThan1000Objects", test: testListMoreThan1000Objects},
	}

	for _, tc := range testCases {
		// Need to empty bucket before each test
		infos, err := client.ListObjectsWithPrefix(nil)
		require.NoError(t, err)

		log.Infof("before running test: %s infos len %d", tc.testName, len(infos))

		if len(infos) > 0 {
			keys := make([][]byte, len(infos))
			for i, info := range infos {
				keys[i] = info.Key
			}
			err = client.DeleteAll(keys)
			require.NoError(t, err)
		}
		t.Run(tc.testName, func(t *testing.T) {
			log.Infof("running test: %s", tc.testName)
			tc.test(t, client)
		})
	}
}

type testCase struct {
	testName string
	test     func(t *testing.T, client Client)
}

func testPutGet(t *testing.T, client Client) {
	vb, err := client.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, vb)

	vb, err = client.Get([]byte("key2"))
	require.NoError(t, err)
	require.Nil(t, vb)

	vb, err = client.Get([]byte("key3"))
	require.NoError(t, err)
	require.Nil(t, vb)

	err = client.Put([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	vb, err = client.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Put([]byte("key2"), []byte("val2"))
	require.NoError(t, err)

	vb, err = client.Get([]byte("key2"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))

	vb, err = client.Get([]byte("key3"))
	require.NoError(t, err)
	require.Nil(t, vb)
}

func testPutOverwrite(t *testing.T, client Client) {
	err := client.Put([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	vb, err := client.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Put([]byte("key1"), []byte("val2"))
	require.NoError(t, err)

	vb, err = client.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))
}

func testDelete(t *testing.T, client Client) {
	err := client.Put([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	vb, err := client.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	err = client.Delete([]byte("key1"))
	require.NoError(t, err)

	vb, err = client.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, vb)
}

func testDeleteAll(t *testing.T, client Client) {

	numKeys := 10
	var keys [][]byte
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		err := client.Put(key, []byte(fmt.Sprintf("val%d", i)))
		keys = append(keys, key)
		require.NoError(t, err)
	}

	// delete some of them

	err := client.DeleteAll(keys[2:7])
	require.NoError(t, err)

	for i, key := range keys {
		v, err := client.Get(key)
		require.NoError(t, err)
		if i >= 2 && i < 7 {
			require.Nil(t, v) // should be deleted
		} else {
			require.Equal(t, []byte(fmt.Sprintf("val%d", i)), v)
		}
	}
}

func testPutIfNotExists(t *testing.T, client Client) {
	err := client.Put([]byte("key1"), []byte("val1"))
	require.NoError(t, err)

	ok, err := client.PutIfNotExists([]byte("key1"), []byte("val2"))
	require.NoError(t, err)
	require.False(t, ok)

	vb, err := client.Get([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val1", string(vb))

	ok, err = client.PutIfNotExists([]byte("key2"), []byte("val2"))
	require.NoError(t, err)
	require.True(t, ok)

	vb, err = client.Get([]byte("key2"))
	require.NoError(t, err)
	require.NotNil(t, vb)
	require.Equal(t, "val2", string(vb))
}

func testListObjects(t *testing.T, client Client) {
	numPrefixes := 10
	numKeys := 10
	var keys [][]byte
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			keys = append(keys, []byte(key))
		}
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	nowMs := time.Now().UnixMilli()
	for _, key := range keys {
		err := client.Put(key, key)
		require.NoError(t, err)
	}
	// note: last-modified in S3 only has millisecond precision
	afterMs := time.Now().UnixMilli()
	for i := 0; i < numPrefixes; i++ {
		prefix := fmt.Sprintf("prefix-%.10d", i)
		infos, err := client.ListObjectsWithPrefix([]byte(prefix))
		require.NoError(t, err)
		require.Equal(t, numKeys, len(infos))
		for j, info := range infos {
			expectedKey := fmt.Sprintf("%s-%.10d", prefix, j)
			require.Equal(t, expectedKey, string(info.Key))
			log.Infof("lm:%d", info.LastModified.Nanosecond())
			require.GreaterOrEqual(t, info.LastModified.UnixMilli(), nowMs)
			require.LessOrEqual(t, info.LastModified.UnixMilli(), afterMs)
		}
	}
	log.Info("done testListObjects")
}

func testListObjectsNilPrefix(t *testing.T, client Client) {
	numPrefixes := 10
	numKeys := 10
	var keys [][]byte
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			key := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			keys = append(keys, []byte(key))
		}
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	nowMs := time.Now().UnixMilli()
	for _, key := range keys {
		err := client.Put(key, key)
		require.NoError(t, err)
	}
	// note: last-modified in S3 only has millisecond precision
	afterMs := time.Now().UnixMilli()
	infos, err := client.ListObjectsWithPrefix(nil)
	require.NoError(t, err)
	require.Equal(t, numKeys*numPrefixes, len(infos))
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numKeys; j++ {
			expectedKey := fmt.Sprintf("prefix-%.10d-%.10d", i, j)
			k := i*numKeys + j
			require.Equal(t, expectedKey, string(infos[k].Key))
			require.GreaterOrEqual(t, infos[k].LastModified.UnixMilli(), nowMs)
			require.LessOrEqual(t, infos[k].LastModified.UnixMilli(), afterMs)
		}
	}
}

// Make sure return more than 1000 - S3 uses pagination and limits max 1000 usually - the client should handle
// calling the S3 API more than once
func testListMoreThan1000Objects(t *testing.T, client Client) {
	numObjects := 2555
	for i := 0; i < numObjects; i++ {
		key := []byte(fmt.Sprintf("key-%.10d", i))
		err := client.Put(key, key)
		require.NoError(t, err)
	}
	infos, err := client.ListObjectsWithPrefix(nil)
	require.NoError(t, err)
	require.Equal(t, numObjects, len(infos))
	for i, info := range infos {
		expectedKey := []byte(fmt.Sprintf("key-%.10d", i))
		require.Equal(t, expectedKey, info.Key)
	}
}
