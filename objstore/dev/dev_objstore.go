package dev

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"time"
)

type Store struct {
	rServer remoting.Server
	store   *InMemStore
}

func NewDevStore(listenAddress string) *Store {
	rServer := remoting.NewServer(listenAddress, conf.TLSConfig{})
	return &Store{
		rServer: rServer,
		store:   NewInMemStore(0),
	}
}

func (d *Store) Start() error {
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreGet, &getMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStorePut, &addMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreDelete, &deleteMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreDeleteAll, &deleteAllMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreListObjectsMessage, &listObjectsHandler{store: d.store})
	return d.rServer.Start()
}

func (d *Store) Stop() error {
	return d.rServer.Stop()
}

type getMessageHandler struct {
	store *InMemStore
}

func (g *getMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreGetRequest)
	value, err := g.store.Get(gm.Key)
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LocalObjStoreGetResponse{Value: value}, nil
}

type addMessageHandler struct {
	store *InMemStore
}

func (a *addMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStorePutRequest)
	var err error
	ok := true
	if gm.IfNotExists {
		ok, err = a.store.PutIfNotExists(gm.Key, gm.Value)
	} else {
		err = a.store.Put(gm.Key, gm.Value)
	}
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LocalObjStorePutResponse{Ok: ok}, nil
}

type deleteMessageHandler struct {
	store *InMemStore
}

func (d *deleteMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreDeleteRequest)
	err := d.store.Delete(gm.Key)
	return nil, err
}

type deleteAllMessageHandler struct {
	store *InMemStore
}

func (d *deleteAllMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	dam := messageHolder.Message.(*clustermsgs.LocalObjStoreDeleteAllRequest)
	err := d.store.DeleteAll(dam.Keys)
	return nil, err
}

type listObjectsHandler struct {
	store *InMemStore
}

func (d *listObjectsHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreListObjectsRequest)
	infos, err := d.store.ListObjectsWithPrefix(gm.Prefix)
	if err != nil {
		return nil, err
	}
	resInfos := make([]*clustermsgs.LocalObjStoreInfoMessage, len(infos))
	for i, info := range infos {
		// Note that last-modified on an S3 object only has millisecond precision so safe to truncate to ms
		resInfos[i] = &clustermsgs.LocalObjStoreInfoMessage{
			Key:          info.Key,
			LastModified: info.LastModified.UnixMilli(),
		}
	}
	return &clustermsgs.LocalObjStoreListObjectsResponse{Infos: resInfos}, nil
}

func NewDevStoreClient(address string) *Client {
	rClient := remoting.NewClient(conf.TLSConfig{})
	return &Client{
		rClient: rClient,
		address: address,
	}
}

type Client struct {
	rClient *remoting.Client
	address string
}

var _ objstore.Client = &Client{}

func (c *Client) Get(key []byte) ([]byte, error) {
	req := &clustermsgs.LocalObjStoreGetRequest{Key: key}
	resp, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return nil, remoting.MaybeConvertError(err)
	}
	vResp := resp.(*clustermsgs.LocalObjStoreGetResponse)
	return vResp.Value, nil
}

func (c *Client) Put(key []byte, value []byte) error {
	req := &clustermsgs.LocalObjStorePutRequest{Key: key, Value: value}
	_, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return remoting.MaybeConvertError(err)
	}
	return nil
}

func (c *Client) PutIfNotExists(key []byte, value []byte) (bool, error) {
	req := &clustermsgs.LocalObjStorePutRequest{Key: key, Value: value, IfNotExists: true}
	resp, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return false, remoting.MaybeConvertError(err)
	}
	vResp := resp.(*clustermsgs.LocalObjStorePutResponse)
	return vResp.Ok, nil
}

func (c *Client) Delete(key []byte) error {
	req := &clustermsgs.LocalObjStoreDeleteRequest{Key: key}
	_, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return remoting.MaybeConvertError(err)
	}
	return nil
}

func (c *Client) DeleteAll(keys [][]byte) error {
	req := &clustermsgs.LocalObjStoreDeleteAllRequest{Keys: keys}
	_, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return remoting.MaybeConvertError(err)
	}
	return nil
}

func (c *Client) ListObjectsWithPrefix(prefix []byte) ([]objstore.ObjectInfo, error) {
	req := &clustermsgs.LocalObjStoreListObjectsRequest{Prefix: prefix}
	resp, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return nil, remoting.MaybeConvertError(err)
	}
	vResp := resp.(*clustermsgs.LocalObjStoreListObjectsResponse)
	infos := make([]objstore.ObjectInfo, len(vResp.Infos))
	for i, info := range vResp.Infos {
		infos[i] = objstore.ObjectInfo{
			Key:          info.Key,
			LastModified: time.UnixMilli(info.LastModified),
		}
	}
	return infos, nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	c.rClient.Stop()
	return nil
}
