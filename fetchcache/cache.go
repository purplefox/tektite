package fetchcache

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/lafikl/consistent"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	lock            sync.RWMutex
	objStore        objstore.Client
	connFactory     transport.ConnectionFactory
	transportServer transport.Server
	cache           *ristretto.Cache
	consist         *consistent.Consistent
	connCaches      map[string]*connectionCache
	members         map[string]struct{}
	dataBucketName  string
	azInfo          string
	stats           CacheStats
}

type CacheStats struct {
	Misses int64
	Hits   int64
}

func NewCache(objStore objstore.Client, connFactory transport.ConnectionFactory,
	transportServer transport.Server, maxSizeBytes int, dataBucketName string, partitionCount int, azInfo string) (*Cache, error) {
	tableSizeEstimate := 16 * 1024 * 1024
	if maxSizeBytes < tableSizeEstimate {
		return nil, errors.Errorf("fetch cache maxSizeBytes must be >= %d", tableSizeEstimate)
	}
	maxItemsEstimate := maxSizeBytes / tableSizeEstimate
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * maxItemsEstimate),
		MaxCost:     int64(maxSizeBytes),
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	// TODO investigate whether this is the right consistent library - it doesn't implement virtual nodes.
	// Do a test with several members and test distribution.
	return &Cache{
		objStore:        objStore,
		connFactory:     connFactory,
		transportServer: transportServer,
		connCaches:      make(map[string]*connectionCache),
		members:         make(map[string]struct{}),
		cache:           cache,
		consist:         consistent.New(),
		dataBucketName:  dataBucketName,
		azInfo:          azInfo,
	}, nil
}

const maxConnectionsPerAddress = 5
const objStoreCallTimeout = 5 * time.Second

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (c *Cache) Start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.transportServer.RegisterHandler(transport.HandlerIDFetchCacheGetTableBytes, c.handleGetTableBytes)
}

func (c *Cache) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, connCache := range c.connCaches {
		connCache.close()
	}
}

func (c *Cache) MembershipChanged(membership cluster.MembershipState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	newMembers := make(map[string]struct{}, len(membership.Members))
	for _, member := range membership.Members {
		newMembers[member.ID] = struct{}{}
		_, exists := c.members[member.ID]
		if !exists {
			// member added
			var membershipData common.MembershipData
			membershipData.Deserialize(member.Data, 0)
			if membershipData.AZInfo == c.azInfo {
				// Each AZ has it's own cache so we don't have cross AZ calls when looking up in cache
				c.consist.Add(membershipData.ListenAddress)
				//c.consist.Add(&strMember{address: membershipData.ListenAddress})
			}
		}
	}
	for member := range c.members {
		_, exists := newMembers[member]
		if !exists {
			// member removed
			c.consist.Remove(member)
		}
	}
	c.members = newMembers
}

type strMember struct {
	address string
}

func (m *strMember) String() string {
	return m.address
}

func (c *Cache) GetTableBytes(key []byte) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	target, err := c.getTargetForKey(key)
	if err != nil {
		return nil, err
	}

	if target == c.transportServer.Address() {
		// Target is this node - we can do a direct call
		return c.getFromCache(key)
	}

	conn, err := c.getConnection(target)
	if err != nil {
		return nil, err
	}

	req := createRequestBuffer()
	req = binary.BigEndian.AppendUint32(req, uint32(len(key)))
	req = append(req, key...)

	resp, err := conn.SendRPC(transport.HandlerIDFetchCacheGetTableBytes, req)
	if err != nil {
		return nil, err
	}
	lb := binary.BigEndian.Uint32(resp)
	bytes := make([]byte, lb)
	copy(bytes, resp[4:4+lb])
	return bytes, nil
}

func (c *Cache) getTargetForKey(key []byte) (string, error) {
	return c.consist.Get(string(key))
}

func (c *Cache) handleGetTableBytes(_ int, request []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	lb := binary.BigEndian.Uint32(request[2:])
	key := request[6 : 6+lb]

	bytes, err := c.getFromCache(key)
	if err != nil {
		return responseWriter(nil, err)
	}
	return sendBytesResponse(responseWriter, responseBuff, bytes)
}

func (c *Cache) getFromCache(key []byte) ([]byte, error) {
	v, ok := c.cache.Get(key)
	if ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		return v.([]byte), nil
	}
	// TODO unavailability retries
	bytes, err := objstore.GetWithTimeout(c.objStore, c.dataBucketName, string(key), objStoreCallTimeout)
	if err != nil {
		return nil, err
	}
	if len(bytes) > 0 {
		atomic.AddInt64(&c.stats.Misses, 1)
		c.cache.Set(key, bytes, int64(len(bytes)))
	}
	return bytes, nil
}

func (c *Cache) GetStats() CacheStats {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stats
}

func sendBytesResponse(responseWriter transport.ResponseWriter, responseBuff []byte, tableBytes []byte) error {
	responseBuff = binary.BigEndian.AppendUint32(responseBuff, uint32(len(tableBytes)))
	responseBuff = append(responseBuff, tableBytes...)
	return responseWriter(responseBuff, nil)
}

func checkRPCVersion(request []byte) error {
	rpcVersion := binary.BigEndian.Uint16(request)
	if rpcVersion != 1 {
		// Currently just 1
		return errors.New("invalid rpc version")
	}
	return nil
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}

func (c *Cache) getConnection(address string) (transport.Connection, error) {
	connCache, ok := c.connCaches[address]
	if !ok {
		connCache = c.createConnCache(address)
	}
	return connCache.getConnection()
}

func (c *Cache) createConnCache(address string) *connectionCache {
	c.lock.RUnlock()
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		c.lock.RLock()
	}()
	connCache, ok := c.connCaches[address]
	if ok {
		return connCache
	}
	connCache = newConnectionCache(address, maxConnectionsPerAddress, c.connFactory)
	c.connCaches[address] = connCache
	return connCache
}

// TODO maybe combine with the similar connectionCache in fetcher package
type connectionCache struct {
	lock        sync.RWMutex
	address     string
	connFactory transport.ConnectionFactory
	connections []*clientWrapper
	pos         int64
}

func newConnectionCache(address string, maxConnections int, connFactory transport.ConnectionFactory) *connectionCache {
	return &connectionCache{
		address:     address,
		connections: make([]*clientWrapper, maxConnections),
		connFactory: connFactory,
	}
}

func (cc *connectionCache) getConnection() (transport.Connection, error) {
	cl, index := cc.getCachedConnection()
	if cl != nil {
		return cl, nil
	}
	return cc.createClient(index)
}

func (cc *connectionCache) getCachedConnection() (*clientWrapper, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.connections)
	return cc.connections[index], index
}

func (cc *connectionCache) createClient(index int) (*clientWrapper, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cl := cc.connections[index]
	if cl != nil {
		return cl, nil
	}
	conn, err := cc.connFactory(cc.address)
	if err != nil {
		return nil, err
	}
	cl = &clientWrapper{
		cc:    cc,
		index: index,
		conn:  conn,
	}
	cc.connections[index] = cl
	return cl, nil
}

func (cc *connectionCache) deleteClient(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.connections[index] = nil
}

func (cc *connectionCache) close() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	for _, client := range cc.connections {
		if client != nil {
			if err := client.conn.Close(); err != nil {
				log.Warnf("failed to close connection: %v", err)
			}
		}
	}
}

type clientWrapper struct {
	cc    *connectionCache
	index int
	conn  transport.Connection
}

func (c *clientWrapper) SendRPC(handlerID int, request []byte) ([]byte, error) {
	return c.conn.SendRPC(handlerID, request)
}

func (c *clientWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.conn.Close()
}
