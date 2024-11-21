package objectstorage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Bucket struct {
	caPEMCerts           []byte
	bucketName, endpoint string
	s3Client             *s3.Client
}

var _ Bucket = &S3Bucket{}

func NewS3Bucket(ctx context.Context, bucketName, endpoint, accessKeyID, secretAccessKey string, caPEMCerts []byte) (*S3Bucket, error) {
	var httpClient config.HTTPClient
	if caPEMCerts != nil {
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caPEMCerts); !ok {
			return nil, errors.New("failed to append certs to pool")
		}
		httpClient = awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			if tr.TLSClientConfig == nil {
				tr.TLSClientConfig = &tls.Config{}
			}
			tr.TLSClientConfig.RootCAs = certPool
		})
	}

	sdkConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithHTTPClient(httpClient),
		config.WithRegion("ceph"),
		config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKeyID,
					SecretAccessKey: secretAccessKey,
				}, nil
			}),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load default config: %w", err)
	}
	s3Client := s3.NewFromConfig(sdkConfig, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	return &S3Bucket{caPEMCerts, bucketName, endpoint, s3Client}, nil
}

func (b *S3Bucket) Exists(ctx context.Context, key string) (bool, error) {
	if _, err := b.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucketName,
		Key:    &key,
	}); err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("HeadObject failed: %s: %s: %s: %w", b.endpoint, b.bucketName, key, err)
	}

	return true, nil
}

func (b *S3Bucket) Delete(ctx context.Context, key string) error {
	if _, err := b.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.bucketName,
		Key:    &key,
	}); err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("Delete failed: %s: %s: %s: %w", b.endpoint, b.bucketName, key, err)
	}
	return nil
}
