package minio

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	miniolib "github.com/minio/minio-go/v7"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"testing"
)

func TestMinioObjStore(t *testing.T) {

	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-26T15-33-07Z", minio.WithUsername("minioadmin"), minio.WithPassword("minioadmin"))
	require.NoError(t, err)

	// Clean up the container
	defer func() {
		err := minioContainer.Terminate(ctx)
		require.NoError(t, err)
	}()

	cfg := &conf.Config{}
	cfg.ApplyDefaults()

	ip, err := minioContainer.Host(ctx)
	require.NoError(t, err)

	port, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)

	cfg.MinioEndpoint = fmt.Sprintf("%s:%d", ip, port.Int())

	cfg.MinioUsername = "minioadmin"
	cfg.MinioPassword = "minioadmin"
	bucketName := fmt.Sprintf("bucket-%s", uuid.New().String())
	cfg.MinioBucketName = bucketName

	client := NewMinioClient(cfg)
	err = client.Start()
	require.NoError(t, err)

	err = client.client.MakeBucket(ctx, bucketName, miniolib.MakeBucketOptions{})
	require.NoError(t, err)

	objstore.TestApi(t, client)
}
