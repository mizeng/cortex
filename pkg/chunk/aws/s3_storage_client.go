package aws

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-kit/kit/log/level"
	"hash/fnv"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
	logUtil "github.com/cortexproject/cortex/pkg/util"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/instrument"
)

var (
	s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func init() {
	s3RequestDuration.Register()
}

type s3ObjectClient struct {
	bucketNames []string
	S3          s3iface.S3API
}

// NewS3ObjectClient makes a new S3-backed ObjectClient.
func NewS3ObjectClient(cfg StorageConfig, schemaCfg chunk.SchemaConfig) (chunk.ObjectClient, error) {
	if cfg.S3.URL == nil {
		return nil, fmt.Errorf("no URL specified for S3")
	}
	s3Config, err := awscommon.ConfigFromURL(cfg.S3.URL)
	if err != nil {
		return nil, err
	}

	s3Config = s3Config.WithS3ForcePathStyle(cfg.S3ForcePathStyle) // support for Path Style S3 url if has the flag

	s3Config = s3Config.WithMaxRetries(0) // We do our own retries, so we can monitor them
	s3Client := s3.New(session.New(s3Config))
	bucketNames := []string{strings.TrimPrefix(cfg.S3.URL.Path, "/")}
	if cfg.BucketNames != "" {
		bucketNames = strings.Split(cfg.BucketNames, ",") // comma separated list of bucket names
	}
	client := s3ObjectClient{
		S3:          s3Client,
		bucketNames: bucketNames,
	}
	return client, nil
}

func (a s3ObjectClient) Stop() {
}

func (a s3ObjectClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, a.getChunk)
}

func (a s3ObjectClient) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	var resp *s3.GetObjectOutput

	// Map the key into a bucket
	namespace := c.Metric.Get("_namespace_")
	if len(namespace) == 0 {
		namespace = "defaultns"
	}
	key := c.ExternalKey()
	bucket := a.bucketFromKey(key) + "_" + namespace // add namespace to bucket name

	level.Debug(logUtil.Logger).Log("msg", fmt.Sprintf("getChunk: key [%s], bucket [%s]\n", key, bucket))

	err := instrument.CollectedRequest(ctx, "S3.GetObject", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		resp, err = a.S3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return err
	})
	if err != nil {
		return chunk.Chunk{}, err
	}
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return chunk.Chunk{}, err
	}
	if err := c.Decode(decodeContext, buf); err != nil {
		return chunk.Chunk{}, err
	}
	return c, nil
}

func (a s3ObjectClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	var (
		namespaces 	[]string
		s3ChunkKeys []string
		s3ChunkBufs [][]byte
	)

	for i := range chunks {
		buf, err := chunks[i].Encoded()
		if err != nil {
			return err
		}

		namespace := chunks[i].Metric.Get("_namespace_")
		if len(namespace) == 0 {
			namespace = "defaultns"
		}
		namespaces = append(namespaces, namespace)
		key := chunks[i].ExternalKey() // add namespace to key

		s3ChunkKeys = append(s3ChunkKeys, key)
		s3ChunkBufs = append(s3ChunkBufs, buf)
	}

	incomingErrors := make(chan error)
	for i := range s3ChunkBufs {
		go func(i int) {
			incomingErrors <- a.putS3Chunk(ctx, namespaces[i], s3ChunkKeys[i], s3ChunkBufs[i])
		}(i)
	}

	var lastErr error
	for range s3ChunkKeys {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (a s3ObjectClient) putS3Chunk(ctx context.Context, namespace, key string, buf []byte) error {
	return instrument.CollectedRequest(ctx, "S3.PutObject", s3RequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		desiredBucket := a.bucketFromKey(key) + "_" + namespace

		level.Debug(logUtil.Logger).Log("msg", fmt.Sprintf("putS3Chunk: bucket [%s]\n", desiredBucket))

		_, err := a.S3.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Body:   bytes.NewReader(buf),
			Bucket: aws.String(a.bucketFromKey(key) + "_" + namespace),
			Key:    aws.String(key),
		})

		if err != nil { // most of the error will be "bucket does not exist". if so, we create the bucket and try flush again
			foundBucket := false
			// check if bucket exists or not
			existBuckets, err := a.S3.ListBuckets(&s3.ListBucketsInput{})
			for _, b := range existBuckets.Buckets {
				bucket := aws.StringValue(b.Name)
				if bucket == desiredBucket {
					foundBucket = true
					break
				}
			}
			// if not found bucket, create one
			if !foundBucket {
				level.Debug(logUtil.Logger).Log("msg", fmt.Sprintf("putS3Chunk: creating bucket [%s]\n", desiredBucket))

				_, err = a.S3.CreateBucket(&s3.CreateBucketInput{
					CreateBucketConfiguration: &s3.CreateBucketConfiguration{LocationConstraint: aws.String("")},
					Bucket: aws.String(a.bucketFromKey(key) + "_" + namespace),
				})

				if err != nil {
					level.Error(logUtil.Logger).Log("msg", fmt.Sprintf("putS3Chunk: create bucket [%s] failed\n", desiredBucket))
					return err
				}

				/* below seems not working in Ceph S3 Object Store
				level.Info(logUtil.Logger).Log("msg", fmt.Sprintf("putS3Chunk: setting expiration on bucket [%s]\n", desiredBucket))

				// set expiration, default is 1 day
				result, err := a.S3.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
					Bucket:                 aws.String(a.bucketFromKey(key) + "_" + namespace),
					LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
						Rules: []*s3.LifecycleRule{
							{Status: aws.String(s3.ExpirationStatusEnabled),
								Expiration: &s3.LifecycleExpiration{Days: aws.Int64(1)},
								Filter: &s3.LifecycleRuleFilter{Prefix: aws.String("")}},
						},
					},
				})

				if err != nil {
					level.Error(logUtil.Logger).Log("msg", fmt.Sprintf("putS3Chunk: set expiracy on bucket [%s] failed\n", desiredBucket))
					return err
				}

				level.Info(logUtil.Logger).Log("msg", "putS3Chunk: setting expiration on bucket with result:\n")
				level.Info(logUtil.Logger).Log("msg", result)
				*/

				// try put object again
				_, err = a.S3.PutObjectWithContext(ctx, &s3.PutObjectInput{
					Body:   bytes.NewReader(buf),
					Bucket: aws.String(a.bucketFromKey(key) + "_" + namespace),
					Key:    aws.String(key),
				})
			}
		}
		return err
	})
}

// bucketFromKey maps a key to a bucket name
func (a s3ObjectClient) bucketFromKey(key string) string {
	if len(a.bucketNames) == 0 {
		return ""
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()

	return a.bucketNames[hash%uint32(len(a.bucketNames))]
}
