package shard

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/transport"
)

type Client interface {

	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	GetOffset(topicID int, partitionID int) (int64, error)
}

// client - note this is not goroutine safe!
// on error, the caller must close the connection
type client struct {
	m *Manager
	shardID int
	address string
	conn transport.Connection
	connFactory transport.ConnectionFactory
	closed bool
}

func (c *client) getConnection() (transport.Connection, error) {
	if c.closed {
		return nil, errors.New("client has been closed")
	}
	if c.conn != nil {
		return c.conn, nil
	}
	if c.address == "" {
		address, err := c.m.addressForShard(c.shardID)
		if err != nil {
			return nil, err
		}
		c.address = address
	}
	conn, err := c.connFactory(c.address)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	c.address = ""
	c.closed = true
	return nil
}

func (c *client) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := ApplyChangesRequest{
		ShardID:  c.shardID,
		RegBatch: regBatch,
	}
	request := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDShardApplyChanges, request)
	return err
}

func (c *client) GetOffset(topicID int, partitionID int) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128) // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}

