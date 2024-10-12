package fetchcache

import (
	"encoding/binary"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/cluster"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/transport"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	lock            sync.RWMutex
	address         string
	objStore        objstore.Client
	connFactory     transport.ConnectionFactory
	transportServer transport.Server
	cache           *ristretto.Cache
	consist         *consistent.Consistent
	dataBucketName  string
	connCaches      map[string]*connectionCache
	members         map[string]struct{}
}

// TODO - Availability zone awareness
func NewCache(address string, objStore objstore.Client, connFactory transport.ConnectionFactory,
	transportServer transport.Server, maxSizeBytes int, dataBucketName string, partitionCount int) (*Cache, error) {
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

	cfg := consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: 1,
		Load:              1.25,
		Hasher:            hasher{},
	}
	consist := consistent.New(nil, cfg)

	return &Cache{
		address:         address,
		objStore:        objStore,
		connFactory:     connFactory,
		transportServer: transportServer,
		cache:           cache,
		consist:         consist,
		dataBucketName:  dataBucketName,
		connCaches:      make(map[string]*connectionCache),
		members:         make(map[string]struct{}),
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
	c.transportServer.RegisterHandler(transport.HandlerIDControllerRegisterL0Table, c.handleGetTableBytes)
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
		newMembers[member.Address] = struct{}{}
		_, exists := c.members[member.Address]
		if !exists {
			// member added
			c.consist.Add(&strMember{address: member.Address})
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

	target := c.getTargetForKey(key)
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

func (c *Cache) getTargetForKey(key []byte) string {
	return c.consist.LocateKey(key).String()
}

func (c *Cache) handleGetTableBytes(_ int, request []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if err := checkRPCVersion(request); err != nil {
		return responseWriter(nil, err)
	}
	lb := binary.BigEndian.Uint32(request[2:])
	key := string(request[6 : 6+lb])

	v, ok := c.cache.Get(key)
	if ok {
		tableBytes := v.([]byte)
		return sendBytesResponse(responseWriter, responseBuff, tableBytes)
	}

	// TODO unavailability retries
	bytes, err := objstore.GetWithTimeout(c.objStore, c.dataBucketName, key, objStoreCallTimeout)
	if err != nil {
		return err
	}
	if len(bytes) > 0 {
		c.cache.Set(key, bytes, int64(len(bytes)))
	}
	return sendBytesResponse(responseWriter, responseBuff, bytes)
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
	lock          sync.RWMutex
	address       string
	clientFactory transport.ConnectionFactory
	clients       []transport.Connection
	pos           int64
}

func newConnectionCache(address string, maxConnections int, connFactory transport.ConnectionFactory) *connectionCache {
	return &connectionCache{
		address:       address,
		clients:       make([]transport.Connection, maxConnections),
		clientFactory: connFactory,
	}
}

func (cc *connectionCache) getConnection() (transport.Connection, error) {
	cl, index := cc.getCachedConnection()
	if cl != nil {
		return cl, nil
	}
	return cc.createClient(index)
}

func (cc *connectionCache) getCachedConnection() (transport.Connection, int) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	pos := atomic.AddInt64(&cc.pos, 1) - 1
	index := int(pos) % len(cc.clients)
	return cc.clients[index], index
}

func (cc *connectionCache) createClient(index int) (transport.Connection, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cl := cc.clients[index]
	if cl != nil {
		return cl, nil
	}
	cl, err := cc.clientFactory(cc.address)
	if err != nil {
		return nil, err
	}
	cc.clients[index] = &clientWrapper{
		cc:     cc,
		index:  index,
		client: cl,
	}
	return cl, nil
}

func (cc *connectionCache) deleteClient(index int) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	cc.clients[index] = nil
}

func (cc *connectionCache) close() {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	for _, cc := range cc.clients {
		if err := cc.Close(); err != nil {
			log.Warnf("failed to close controller client: %v", err)
		}
	}
}

type clientWrapper struct {
	cc     *connectionCache
	index  int
	client transport.Connection
}

func (c *clientWrapper) SendRPC(handlerID int, request []byte) ([]byte, error) {
	return c.client.SendRPC(handlerID, request)
}

func (c *clientWrapper) Close() error {
	c.cc.deleteClient(c.index)
	return c.client.Close()
}
