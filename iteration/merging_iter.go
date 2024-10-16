package iteration

import (
	"bytes"
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"reflect"
	"sync"
)

type MergingIterator struct {
	highestVersion           uint64
	iters                    []Iterator
	dropIterCurrent          []bool
	preserveTombstones       bool
	current                  common.KV
	currIndex                int
	minNonCompactableVersion uint64
	noDropOnNext             bool
	currentTombstone         []byte
	isPrefixTombstone        bool

	lastKey   []byte
	lastValue []byte

	nonCompactableKeyNoVersion []byte
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool, highestVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           highestVersion,
		minNonCompactableVersion: math.MaxUint64,
		iters:                    iters,
		dropIterCurrent:          make([]bool, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func NewCompactionMergingIterator(iters []Iterator, preserveTombstones bool, minNonCompactableVersion uint64) (*MergingIterator, error) {
	mi := &MergingIterator{
		highestVersion:           math.MaxUint64,
		minNonCompactableVersion: minNonCompactableVersion,
		iters:                    iters,
		dropIterCurrent:          make([]bool, len(iters)),
		preserveTombstones:       preserveTombstones,
	}
	return mi, nil
}

func (m *MergingIterator) Next() (bool, common.KV, error) {
	var lastKeyNoVersion []byte
	if len(m.current.Key) > 0 {
		lastKeyNoVersion = m.current.Key[:len(m.current.Key)-8]
		if encoding.DecodeKeyVersion(m.current.Key) >= m.minNonCompactableVersion {
			// Cannot compact it
			// We set this flag to mark that we cannot drop any other proceeding same keys with lower versions either
			// If the first one is >= minNonCompactable but proceeding lower keys are < minNonCompactable they can't be
			// dropped either otherwise on rollback of snapshot we could be left with no versions of those keys.
			m.noDropOnNext = true
		}
	}

	repeat := true
	for repeat {
		// Now find the smallest key from all iterators, choosing the highest version when keys draw
		var keyNoVersion, chosenKeyNoVersion []byte
		var smallestIndex int
		var choosen common.KV
		var version, choosenVersion uint64
		var err error
		var valid bool
	outer:
		for i := range m.iters {
			c := m.iters[i].Current()
			if len(c.Key) == 0 || m.dropIterCurrent[i] {
				for {
					// Find the next valid iter's KV that is compactable and has a version that is not too high
					valid, c, err = m.iters[i].Next()
					if err != nil {
						return false, common.KV{}, err
					}
					if !valid {
						continue outer
					}

					version = encoding.DecodeKeyVersion(c.Key)

					// Skip over same key (if it's compactable)
					// in same iterator we can have multiple versions of the same key
					keyNoVersion = c.Key[:len(c.Key)-8]
					if bytes.Equal(lastKeyNoVersion, keyNoVersion) {
						// key is same
						if version < m.minNonCompactableVersion {
							// theoretically compactable
							if !m.noDropOnNext {
								// we can't drop it though because the highest version of this same key was not compactable,
								// therefore we can't compact any other versions of it either
								if log.DebugEnabled {
									lastVersion := encoding.DecodeKeyVersion(m.current.Key)
									log.Debugf("%p mi: dropping key in next as same key: key %v (%s) value %v (%s) version:%d last key: %v (%s) last value %v (%s) last version %d minnoncompactableversion:%d",
										m, c.Key, string(c.Key), c.Value, string(c.Value), version, m.current.Key, string(m.current.Key), m.current.Value, string(m.current.Value), lastVersion, m.minNonCompactableVersion)
								}
								continue
							}
						}
					} else {
						m.noDropOnNext = false
					}

					// Skip past keys with too high a version
					// prefix tombstones always have version math.MaxUint64 and are never screened out

					if version > m.highestVersion && version != math.MaxUint64 {
						if log.DebugEnabled {
							log.Debugf("%p merging iter skipping past key %v (%s) as version %d too high - max version %d",
								m, c.Key, string(c.Key), version, m.highestVersion)
						}
						continue
					}
					m.dropIterCurrent[i] = false
					break
				}
			} else {
				version = encoding.DecodeKeyVersion(c.Key)
				keyNoVersion = c.Key[:len(c.Key)-8]
			}

			if chosenKeyNoVersion == nil {
				chosenKeyNoVersion = keyNoVersion
				smallestIndex = i
				choosen = c
				choosenVersion = version
			} else {
				diff := bytes.Compare(keyNoVersion, chosenKeyNoVersion)
				if diff < 0 {
					chosenKeyNoVersion = keyNoVersion
					smallestIndex = i
					choosen = c
					choosenVersion = version
				} else if diff == 0 {
					// Keys are same

					// choose the highest version, and drop the other one as long as the highest version < minNonCompactable
					if version > choosenVersion {
						// the current version is higher so drop the previous highest if the current version is compactable
						// note we can only drop the previous highest if *this* version is compactable as dropping it
						// will leave this version, and if its non compactable it means that snapshot rollback could
						// remove it, which would leave nothing.
						if version < m.minNonCompactableVersion {
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 1, m.minNonCompactableVersion, choosenVersion, c.Key, string(c.Key), c.Value, string(c.Value))
							}
							m.dropIterCurrent[smallestIndex] = true
						}
						chosenKeyNoVersion = keyNoVersion
						smallestIndex = i
						choosen = c
						choosenVersion = version
					} else if version < choosenVersion {
						// the previous highest version is higher than this version, so we can remove this version
						// as long as previous highest is compactable
						if choosenVersion < m.minNonCompactableVersion {
							// drop this entry if the version is compactable
							if log.DebugEnabled {
								log.Debugf("%p mi: dropping as key version %d less than minnoncompactable (%d) %d chosenKeyVersion %d: key %v (%s) value %v (%s)",
									m, version, 2, m.minNonCompactableVersion, choosenVersion, choosen.Key, string(choosen.Key), choosen.Value, string(choosen.Value))
							}
							m.dropIterCurrent[i] = true
						}
					} else {
						// same key, same version, drop this one, and keep the one we already found,
						if log.DebugEnabled {
							log.Debugf("%p mi: dropping key as same key and version: key %v (%s) value %v (%s) ver %d",
								m, c.Key, string(c.Key), c.Value, string(c.Value), version)
						}
						m.dropIterCurrent[i] = true
					}
				}
			}
		}

		if chosenKeyNoVersion == nil {
			// Nothing valid
			return false, common.KV{}, nil
		}

		if m.currentTombstone != nil {
			if bytes.Equal(m.currentTombstone, choosen.Key[:len(m.currentTombstone)]) {
				if m.isPrefixTombstone || choosenVersion < m.minNonCompactableVersion {
					// The key matches current prefix tombstone
					// skip past it if it is compactable - for prefixes we alwqays delete even if non compactable
					m.dropIterCurrent[smallestIndex] = true
					continue
				}
			} else {
				// does not match - reset the prefix tombstone
				m.currentTombstone = nil
				m.isPrefixTombstone = false
			}
		}

		isTombstone := len(choosen.Value) == 0
		if isTombstone {
			// We have a tombstone, keep track of it. Prefix tombstones (used for deletes of partitions)
			// are identified by having a version of math.MaxUint64
			m.currentTombstone = chosenKeyNoVersion
			if choosenVersion == math.MaxUint64 {
				m.isPrefixTombstone = true
			}
		}
		if !m.preserveTombstones && (isTombstone || choosenVersion == math.MaxUint64) {
			// We have a tombstone or a prefix tombstone end marker - skip past it
			// End marker also is identified as having a version of math.MaxUint64
			m.dropIterCurrent[smallestIndex] = true
		} else {
			// output the entry
			m.current.Key = choosen.Key
			m.current.Value = choosen.Value
			m.currIndex = smallestIndex
			repeat = false
		}
	}
	m.dropIterCurrent[m.currIndex] = true

	//if len(m.current.Key) > 0 {
	//	slabID := binary.BigEndian.Uint64(m.current.Key[16:])
	//	if slabID >= common.UserSlabIDBase {
	//		log.Debugf("merging iter outputting val %s ver1 %d minNonCompactable %d", convBytesToString(m.current.Value), encoding.DecodeKeyVersion(m.current.Key), m.minNonCompactableVersion)
	//	}
	//}

	//slabID := binary.BigEndian.Uint64(m.current.Key[16:])
	//if slabID >= common.UserSlabIDBase {
	//	if m.lastKey != nil && bytes.Equal(m.lastKey[:len(m.lastKey)-8], m.current.Key[:len(m.current.Key)-8]) {
	//		log.Debugf("merging iter outputting duplicate key v1 %s ver1 %d v2 %s ver2 %d", convBytesToString(m.lastValue), encoding.DecodeKeyVersion(m.lastKey), convBytesToString(m.current.Value), encoding.DecodeKeyVersion(m.current.Key))
	//		m.Dump()
	//	}
	//}
	//m.lastKey = m.current.Key
	//m.lastValue = m.current.Value
	return true, m.current, nil
}

func (m *MergingIterator) isCompactable(keyNoVersion []byte, version uint64) bool {
	if m.nonCompactableKeyNoVersion != nil {
		if bytes.Equal(m.nonCompactableKeyNoVersion, keyNoVersion) {
			// If we've seen a key with a non compactable version then all versions of the same key are also
			// non compactable even if their versions are < minNonCompactableVersion
			return false
		} else {
			// Key has changed - reset nonCompactableKeyNoVersion
			m.nonCompactableKeyNoVersion = nil
		}
	}
	if version < m.minNonCompactableVersion {
		// Compactable and no nonCompactable version of the same key
		return true
	}
	// Not compactable - save the key as all versions of this key will be non compactable too
	m.nonCompactableKeyNoVersion = keyNoVersion
	return false
}

var lock sync.Mutex

func (m *MergingIterator) Dump() {
	lock.Lock()
	defer lock.Unlock()
	log.Debugf("Dumping merging iter, has %d iters", len(m.iters))
	for i, iter := range m.iters {
		log.Debugf("iter %d is of type %s", i, reflect.TypeOf(iter).String())
		for {
			ok, curr, err := iter.Next()
			if err != nil {
				panic(err)
			}
			if !ok {
				break
			}
			slabID := binary.BigEndian.Uint64(curr.Key[16:])
			if slabID >= common.UserSlabIDBase {
				log.Debugf("iter has key %s value %s version %d", convBytesToString(curr.Key), convBytesToString(curr.Value), encoding.DecodeKeyVersion(curr.Key))
			}
		}
	}
}

func convBytesToString(bytes []byte) string {
	lb := len(bytes)
	out := make([]byte, lb)
	for i, b := range bytes {
		if b >= 32 && b <= 126 {
			// we only display printable ASCII chars
			out[i] = b
		} else {
			out[i] = '.'
		}
	}
	return string(out)
}
func (m *MergingIterator) Current() common.KV {
	return m.current
}

func (m *MergingIterator) Close() {
	for _, iter := range m.iters {
		iter.Close()
	}
}
