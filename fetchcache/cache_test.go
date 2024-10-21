package fetchcache

import (
	"context"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCacheSingleNode(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	localTransports := transport.NewLocalTransports()
	transportServer, err := localTransports.NewLocalServer("server-address-1")
	require.NoError(t, err)

	cfg := NewConf()
	cfg.DataBucketName = "test-bucket"
	cfg.AzInfo = "test-az"
	cfg.MaxSizeBytes = 16 * 1024 * 1024
	cfg.MaxConnectionsPerAddress = 100

	cache, err := NewCache(objStore, localTransports.CreateConnection, transportServer, cfg)
	require.NoError(t, err)

	cache.Start()
	defer cache.Stop()

	membershipData := common.MembershipData{
		ListenAddress: transportServer.Address(),
		AZInfo:        cfg.AzInfo,
	}
	cache.MembershipChanged(cluster.MembershipState{
		ClusterVersion: 1,
		Members: []cluster.MembershipEntry{
			{
				ID:   uuid.New().String(),
				Data: membershipData.Serialize(nil),
			},
		},
	})

	stats := cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 0, int(stats.Misses))

	key1 := []byte("some-key-1")
	bytes, err := cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, 0, len(bytes))

	stats = cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 0, int(stats.Misses))

	tableBytes := []byte("quwdhiquwhdiquwhdiuqd")
	err = objStore.Put(context.Background(), cfg.DataBucketName, string(key1), tableBytes)
	require.NoError(t, err)

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 0, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))

	// ristretto has async put
	cache.cache.Wait()

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 1, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))

	bytes, err = cache.GetTableBytes(key1)
	require.NoError(t, err)
	require.Equal(t, tableBytes, bytes)

	stats = cache.GetStats()
	require.Equal(t, 2, int(stats.Hits))
	require.Equal(t, 1, int(stats.Misses))
}
