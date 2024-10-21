package fetchcache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type ConsistentHash struct {
	lock          sync.RWMutex
	virtualFactor int
	hashMemberMap map[uint64]string
	memberHashes  map[string][]uint64
	sorted        []uint64
}

func NewConsistentHash(virtualFactor int) *ConsistentHash {
	return &ConsistentHash{
		virtualFactor: virtualFactor,
		hashMemberMap: map[uint64]string{},
		memberHashes:  map[string][]uint64{},
	}
}

func (c *ConsistentHash) Add(member string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	hashes := make([]uint64, c.virtualFactor)
	for i := 0; i < c.virtualFactor; i++ {
		//key := []byte(uuid.New().String())
		key := []byte(fmt.Sprintf("%s-%d", member, i))
		h := hash(key)
		hashes[i] = h
		_, exists := c.hashMemberMap[h]
		if exists {
			// should never happen with sha-256
			panic("hash collision")
		}
		c.hashMemberMap[h] = member
	}
	c.memberHashes[member] = hashes
	c.createSortedKeys()
}

func (c *ConsistentHash) Remove(member string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	hashes, ok := c.memberHashes[member]
	if !ok {
		return false
	}
	for _, h := range hashes {
		delete(c.hashMemberMap, h)
	}
	c.createSortedKeys()
	return true
}

func (c *ConsistentHash) createSortedKeys() {
	all := make([]uint64, 0, len(c.hashMemberMap))
	for h := range c.hashMemberMap {
		all = append(all, h)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i] < all[j]
	})
	c.sorted = all
}

func (c *ConsistentHash) Get(key []byte) (string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.sorted) == 0 {
		return "", false
	}
	h := hash(key)
	pos := sort.Search(len(c.sorted), func(x int) bool {
		return c.sorted[x] > h
	})
	if pos >= len(c.sorted) {
		pos = 0
	}
	v := c.sorted[pos]
	member := c.hashMemberMap[v]
	return member, true
}

func hash(key []byte) uint64 {
	h := sha256.New()
	if _, err := h.Write(key); err != nil {
		panic(err)
	}
	bytes := h.Sum(nil)
	return binary.BigEndian.Uint64(bytes)
}
