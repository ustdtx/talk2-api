package media

// Storage mirrors Rewardly-apiV2's IStorageService:
// backend-proxied PutObject to R2, returns the public URL.
// R2 quirks (same as Rewardly's S3Storage):
//   - path-style URLs against the account endpoint
//   - no flexible checksums (R2 rejects streaming-with-trailer signatures)

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrNotConfigured is returned when S3/R2 settings are missing,
// mirroring Rewardly's MissingStorageService (HTTP 503).
var ErrNotConfigured = errors.New("file storage is not configured (S3/R2)")

type Settings struct {
	Bucket          string
	Region          string
	EndpointURL     string
	PublicURL       string
	AccessKeyID     string
	SecretAccessKey string
}

func (s Settings) Configured() bool {
	return s.Bucket != "" && s.EndpointURL != "" && s.AccessKeyID != "" && s.SecretAccessKey != ""
}

type Storage interface {
	Upload(ctx context.Context, key string, body io.Reader, contentType string) (publicURL string, err error)
	Delete(ctx context.Context, key string) error
	// DeleteMany is fan-out parallel delete for wipe paths (offline wipe, thread wipe).
	DeleteMany(ctx context.Context, keys []string) error
	PublicURL(key string) string
}

type S3Storage struct {
	client *s3.Client
	bucket string
	public string
}

// NewS3Storage builds the R2 client. Same recipe as Rewardly's
// S3Storage.CreateClient: ServiceURL + ForcePathStyle + checksums only when required.
func NewS3Storage(s Settings) *S3Storage {
	region := s.Region
	if region == "" {
		region = "auto"
	}
	cfg := aws.Config{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(s.AccessKeyID, s.SecretAccessKey, ""),
		BaseEndpoint: &s.EndpointURL,
		// R2 rejects the streaming-with-trailer checksum signature that newer
		// SDKs send by default; only add checksums where the API requires them.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return &S3Storage{client: client, bucket: s.Bucket, public: strings.TrimRight(s.PublicURL, "/")}
}

func (s *S3Storage) PublicURL(key string) string { return s.public + "/" + key }

func (s *S3Storage) Upload(ctx context.Context, key string, body io.Reader, contentType string) (string, error) {
	if contentType == "" {
		contentType = "image/jpeg"
	}
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", err
	}
	return s.PublicURL(key), nil
}

func (s *S3Storage) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *S3Storage) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	const fanout = 10
	sem := make(chan struct{}, fanout)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.Delete(ctx, key); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("delete %s: %w", key, err)
				}
				mu.Unlock()
			}
		}(k)
	}
	wg.Wait()
	return firstErr
}

// MissingStorage mirrors Rewardly's MissingStorageService: every op is a 503.
type MissingStorage struct{}

func (MissingStorage) PublicURL(key string) string { return "" }
func (MissingStorage) Upload(_ context.Context, _ string, _ io.Reader, _ string) (string, error) {
	return "", ErrNotConfigured
}
func (MissingStorage) Delete(_ context.Context, _ string) error { return ErrNotConfigured }
func (MissingStorage) DeleteMany(_ context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return ErrNotConfigured
}
