package controller

import (
	"github.com/graph-gophers/graphql-go/errors"
	"sync"
)

type LocalTransports struct {
	lock       sync.RWMutex
	transports map[string]MessageHandler
}

func (lt *LocalTransports) deliverMessage(address string, message []byte) error {
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	handler, ok := lt.transports[address]
	if !ok {
		return errors.Errorf("no handler found for address %s", address)
	}
	return handler(message)
}

func (lt *LocalTransports) NewLocalTransport(address string) (*LocalTransport, error) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	_, exists := lt.transports[address]
	if exists {
		return nil, errors.Errorf("handler already exists for address %s", address)
	}
	transport := &LocalTransport{
		address:    address,
		transports: lt,
	}
	lt.transports[address] = transport.handleMessage
	return transport, nil
}

type LocalTransport struct {
	address    string
	handler    MessageHandler
	transports *LocalTransports
}

func (l *LocalTransport) handleMessage(message []byte) error {
	return l.handler(message)
}

func (l *LocalTransport) CreateConnection(address string, handler MessageHandler) (Connection, error) {
	return &LocalConnection{transport: l}, nil
}

func (l *LocalTransport) RegisterHandler(handler MessageHandler) {
	l.handler = handler
}

type LocalConnection struct {
	transport *LocalTransport
}

func (l *LocalConnection) WriteMessage(message []byte) error {
	return l.transport.transports.deliverMessage(l.transport.address, message)
}

func (l *LocalConnection) Close() error {
	//TODO implement me
	panic("implement me")
}
