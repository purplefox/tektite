package minio

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"io"
)

func NewMinioClient(cfg *conf.Config) *Client {
	return &Client{
		cfg: cfg,
	}
}

type Client struct {
	cfg    *conf.Config
	client *minio.Client
}

func (m *Client) ListObjectsWithPrefix(prefix []byte) ([]objstore.ObjectInfo, error) {
	var opts minio.ListObjectsOptions
	opts.Prefix = string(prefix)
	log.Infof("calling ListObjects with prefix '%s'", opts.Prefix)
	ch := m.client.ListObjects(context.Background(), m.cfg.MinioBucketName, opts)
	var infos []objstore.ObjectInfo
	for info := range ch {
		if info.Err != nil {
			return nil, maybeConvertError(info.Err)
		}
		infos = append(infos, objstore.ObjectInfo{
			Key:          []byte(info.Key),
			LastModified: info.LastModified,
		})
	}
	log.Infof("called ListObjects with prefix '%s'", opts.Prefix)
	return infos, nil
}

func (m *Client) Get(key []byte) ([]byte, error) {
	objName := string(key)
	obj, err := m.client.GetObject(context.Background(), m.cfg.MinioBucketName, objName, minio.GetObjectOptions{})
	if err != nil {
		return nil, maybeConvertError(err)
	}
	//goland:noinspection GoUnhandledErrorResult
	defer obj.Close()
	buff, err := io.ReadAll(obj)
	if err != nil {
		var merr minio.ErrorResponse
		if errwrap.As(err, &merr) {
			if merr.StatusCode == 404 {
				// does not exist
				return nil, nil
			}
		}
		return nil, maybeConvertError(err)
	}
	return buff, nil
}

func (m *Client) Put(key []byte, value []byte) error {
	buff := bytes.NewBuffer(value)
	objName := string(key)
	_, err := m.client.PutObject(context.Background(), m.cfg.MinioBucketName, objName, buff, int64(len(value)),
		minio.PutObjectOptions{})
	return maybeConvertError(err)
}

func (m *Client) PutIfNotExists(key []byte, value []byte) (bool, error) {
	buff := bytes.NewBuffer(value)
	objName := string(key)
	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")
	_, err := m.client.PutObject(context.Background(), m.cfg.MinioBucketName, objName, buff, int64(len(value)), opts)
	if err != nil {
		var errResponse minio.ErrorResponse
		if errors.As(err, &errResponse) {
			if errResponse.StatusCode == 412 {
				// Pre-condition failed - this means key already exists
				return false, nil
			}
		}
		return false, maybeConvertError(err)
	}
	return true, nil
}

func (m *Client) Delete(key []byte) error {
	objName := string(key)
	return maybeConvertError(m.client.RemoveObject(context.Background(), m.cfg.MinioBucketName, objName, minio.RemoveObjectOptions{}))
}

func (m *Client) DeleteAll(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	opts := minio.RemoveObjectsOptions{}
	// must be a blocking channel
	ch := make(chan minio.ObjectInfo)
	errCh := m.client.RemoveObjects(context.Background(), m.cfg.MinioBucketName, ch, opts)
	// Minio client has a weird API here forcing us to spawn GRs, and add messages to channel after calling RemoveObjects
	// to avoid losing any messages
	go func() {
		for _, key := range keys {
			ch <- minio.ObjectInfo{
				Key: string(key),
			}
		}
		close(ch)
	}()
	for err := range errCh {
		if err.Err != nil {
			return err.Err
		}
	}
	return nil
}

func (m *Client) Start() error {
	client, err := minio.New(m.cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(m.cfg.MinioUsername, m.cfg.MinioPassword, ""),
		Secure: m.cfg.MinioSecure,
	})

	if err != nil {
		return err
	}
	m.client = client
	return nil
}

func (m *Client) Stop() error {
	m.client = nil
	return nil
}

func maybeConvertError(err error) error {
	if err == nil {
		return err
	}
	return common.NewTektiteErrorf(common.Unavailable, err.Error())
}
