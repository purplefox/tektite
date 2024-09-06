package controller

import "github.com/spirit-labs/tektite/replicator"

type OffsetsCommand struct {
}

func (oc *OffsetsCommand) HandleBuffer(buff []byte) (replicator.CommandStatus, error) {
	//TODO implement me
	panic("implement me")
}
