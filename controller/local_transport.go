package controller

import (
	"github.com/graph-gophers/graphql-go/errors"
	"sync"
)

type LocalTransports struct {
	lock       sync.RWMutex
	transports map[string]RequestHandler
}

func (lt *LocalTransports) deliverMessage(address string, message []byte, responseHandler ResponseHandler) error {
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	handler, ok := lt.transports[address]
	if !ok {
		return errors.Errorf("no handler found for address %s", address)
	}
	return handler(message, responseHandler)
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
	lt.transports[address] = transport.handleRequest
	return transport, nil
}

type LocalTransport struct {
	address    string
	handler    RequestHandler
	transports *LocalTransports
}

func (l *LocalTransport) handleRequest(request []byte, responseWriter ResponseHandler) error {
	return l.handler(request, responseWriter)
}

func (l *LocalTransport) CreateConnection(address string, handler ResponseHandler) (Connection, error) {
	return &LocalConnection{
		transport: l,
		handler:   handler,
	}, nil
}

func (l *LocalTransport) RegisterHandler(handler RequestHandler) {
	l.handler = handler
}

type LocalConnection struct {
	transport *LocalTransport
	handler   ResponseHandler
}

func (l *LocalConnection) WriteMessage(message []byte) error {
	return l.transport.transports.deliverMessage(l.transport.address, message, l.handler)
}

func (l *LocalConnection) Close() error {
	//TODO implement me
	panic("implement me")
}
