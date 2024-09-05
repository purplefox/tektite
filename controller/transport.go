package controller

import (
	"github.com/graph-gophers/graphql-go/errors"
	"sync"
)

type Transport interface {
	CreateConnection(address string, handler MessageHandler) (Connection, error)

	RegisterHandler(handler MessageHandler)
}

type MessageHandler func(message []byte) error

type Connection interface {
	WriteMessage(message []byte) error
	Close() error
}
