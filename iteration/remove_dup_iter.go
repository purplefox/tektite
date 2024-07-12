package iteration

import (
	"bytes"
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	log "github.com/spirit-labs/tektite/logger"
)

func NewRemoveDupIter(iter Iterator) *RemoveDupIter {
	return &RemoveDupIter{iter: iter}
}

type RemoveDupIter struct {
	iter    Iterator
	lastKey []byte
}

func (r *RemoveDupIter) Next() (bool, common.KV, error) {
	for {
		valid, curr, err := r.iter.Next()
		if err != nil {
			return false, common.KV{}, err
		}
		if !valid {
			return false, common.KV{}, nil
		}
		if r.lastKey != nil && bytes.Equal(keyNoVersion(r.lastKey), keyNoVersion(curr.Key)) {
			// duplicate - ignore
			continue
		}
		r.lastKey = curr.Key

		if len(curr.Key) > 0 {
			slabID := binary.BigEndian.Uint64(curr.Key[16:])
			if slabID >= common.UserSlabIDBase {
				log.Infof("RemoveDupIter outputting key %v val %s ver1 %d", curr.Key, convBytesToString(curr.Value), encoding.DecodeKeyVersion(curr.Key))
			}
		}

		return true, curr, nil
	}
}

func keyNoVersion(key []byte) []byte {
	return key[:len(key)-8]
}

func (r *RemoveDupIter) Current() common.KV {
	return r.iter.Current()
}

func (r *RemoveDupIter) Close() {
	r.iter.Close()
}
