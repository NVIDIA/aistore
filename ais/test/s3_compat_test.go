// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	aiss3 "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type customTransport struct {
	pathStyle bool
	rt        http.RoundTripper
}

func (t *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.HasPrefix(req.URL.Path, "/s3") {
		if t.pathStyle {
			req.URL.Path = "/s3" + req.URL.Path
		} else {
			bucket := strings.Split(req.URL.Host, ".")[0]
			u, _ := url.Parse(proxyURL)
			req.URL.Host = u.Host
			req.URL.Path = "/s3/" + bucket + req.URL.Path
		}
	}
	return t.rt.RoundTrip(req)
}

type addGetBodyMiddleware struct{}

func (*addGetBodyMiddleware) ID() string {
	return "AddGetBodyMiddleware"
}

func (*addGetBodyMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleFinalize(ctx, in)
	}

	// NOTE: This fixes a problem with `write: connection reset by peer`.
	delete(req.Header, "Expect")

	if req.IsStreamSeekable() {
		req.GetBody = func() (io.ReadCloser, error) {
			if err := req.RewindStream(); err != nil {
				return nil, err
			}
			return io.NopCloser(req.GetStream()), nil
		}
	}
	return next.HandleFinalize(ctx, in)
}

func newCustomTransport(pathStyle bool) *customTransport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &customTransport{
		pathStyle: pathStyle,
		rt: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
}

func newS3Client(pathStyle bool) *http.Client {
	return &http.Client{
		Transport: newCustomTransport(pathStyle),
	}
}

// getS3Credentials returns appropriate credentials based on cluster auth configuration
// If auth is enabled, returns JWT token from authenticated session
// Otherwise returns anonymous credentials
func getS3Credentials(t *testing.T) aws.CredentialsProvider {
	config, err := api.GetClusterConfig(tools.BaseAPIParams())
	tassert.CheckFatal(t, err)

	if config.Auth.Enabled {
		authBP := tools.BaseAPIParams()
		if authBP.Token == "" {
			t.Fatal("Auth is enabled but no token available")
		}
		// Use JWT token via X-Amz-Security-Token header (requires allow_s3_token_compat)
		return credentials.NewStaticCredentialsProvider(
			"dummy-access-key",
			"dummy-secret-key",
			authBP.Token, // JWT as SessionToken
		)
	}

	return aws.AnonymousCredentials{}
}

// setupS3Compat configures the cluster for S3 compatibility tests
// If auth is enabled, it automatically enables S3 reverse proxy and JWT token compat mode
// Returns a cleanup function that restores original settings
func setupS3Compat(t *testing.T) {
	config, err := api.GetClusterConfig(tools.BaseAPIParams())
	tassert.CheckFatal(t, err)

	if !config.Auth.Enabled {
		// No auth - nothing to set up
		return
	}

	// Auth is enabled - ensure S3 JWT compat mode is enabled
	originalFeatures := config.Features.String()
	originalS3TokenCompat := strconv.FormatBool(config.Auth.AllowS3TokenCompat)

	tools.SetClusterConfig(t, cos.StrKVs{
		"features":                   feat.S3ReverseProxy.String(),
		"auth.allow_s3_token_compat": "true",
	})

	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{
			"features":                   originalFeatures,
			"auth.allow_s3_token_compat": originalS3TokenCompat,
		})
	})
}

func setBucketFeatures(t *testing.T, bck cmn.Bck, bprops *cmn.Bprops, nf feat.Flags) {
	if bprops.Features.IsSet(nf) {
		return // nothing to do
	}
	props := &cmn.BpropsToSet{Features: &nf}
	_, err := api.SetBucketProps(baseParams, bck, props)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		// restore original feature flags
		props := &cmn.BpropsToSet{Features: &bprops.Features}
		_, err := api.SetBucketProps(baseParams, bck, props)
		tassert.CheckFatal(t, err)
	})
}

func loadCredentials(t *testing.T) (f func(*config.LoadOptions) error) {
	switch {
	case os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "":
		f = config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
		)
	case os.Getenv("AWS_PROFILE") != "":
		f = config.WithSharedConfigProfile(os.Getenv("AWS_PROFILE"))
	default:
		t.Skip("Failed to load credentials, none of AWS_PROFILE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY are set")
		f = func(*config.LoadOptions) error { return nil }
	}
	return f
}

func TestS3PresignedPutGet(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)
	bprops, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setBucketFeatures(t, bck, bprops, feat.S3PresignedRequest)

	tools.SetClusterConfig(t, cos.StrKVs{"features": feat.S3ReverseProxy.String()})
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
	})

	/* TODO -- FIXME: alternatively, use env vars AWS_PROFILE et al:
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithSharedConfigProfile("default"),
	)
	tassert.CheckFatal(t, err)
	cfg.HTTPClient = newS3Client()
	s3Client := s3.NewFromConfig(cfg)
	*/
	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(false /*pathStyle*/), Region: env.AwsDefaultRegion()})

	putOutput, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   io.LimitReader(rand.Reader, fileSize),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, putOutput.ETag != nil, "ETag for PUT operation was not set")
	tassert.Errorf(t, *putOutput.ETag != "", "ETag for PUT operation is empty")

	getOutput, err := s3Client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, getOutput.ETag != nil, "ETag for PUT operation was not set")
	tassert.Errorf(t, *getOutput.ETag != "", "ETag for PUT operation is empty")

	cos.DrainReader(getOutput.Body)
	getOutput.Body.Close()

	tassert.Errorf(t, *putOutput.ETag == *getOutput.ETag, "ETag does not match between PUT and GET operation (%s != %s)", *putOutput.ETag, *getOutput.ETag)
}

func TestS3PresignedMultipart(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)
	bprops, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setBucketFeatures(t, bck, bprops, feat.S3PresignedRequest)

	tools.SetClusterConfig(t, cos.StrKVs{"features": feat.S3ReverseProxy.String()})
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
	})

	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(false /*pathStyle*/), Region: env.AwsDefaultRegion()})

	createMultipartUploadOutput, err := s3Client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, createMultipartUploadOutput.UploadId != nil, "UploadId for CreateMultipartUpload operation was not set")
	tassert.Errorf(t, *createMultipartUploadOutput.UploadId != "", "UploadId for CreateMultipartUpload operation is empty")

	var parts []types.CompletedPart //nolint:prealloc // Not needed.
	for i := 1; i <= 3; i++ {
		uploadPartOutput, err := s3Client.UploadPart(t.Context(), &s3.UploadPartInput{
			Bucket:        aws.String(bck.Name),
			Key:           aws.String(objName),
			PartNumber:    aws.Int32(int32(i)),
			UploadId:      createMultipartUploadOutput.UploadId,
			Body:          io.LimitReader(rand.Reader, 5*cos.MiB),
			ContentLength: aws.Int64(5 * cos.MiB),
		})
		tassert.CheckFatal(t, err)
		tassert.Errorf(t, uploadPartOutput.ETag != nil, "ETag for UploadPart operation was not set")

		parts = append(parts, types.CompletedPart{
			ETag:       uploadPartOutput.ETag,
			PartNumber: aws.Int32(int32(i)),
		})
	}

	completeMultipartUpload, err := s3Client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bck.Name),
		Key:             aws.String(objName),
		UploadId:        createMultipartUploadOutput.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, completeMultipartUpload.ETag != nil, "ETag for CreateMultipartUpload was not set")

	getOutput, err := s3Client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, getOutput.ETag != nil, "ETag for GET operation was not set")
	tassert.Errorf(t, *getOutput.ETag != "", "ETag for GET operation is empty")

	cos.DrainReader(getOutput.Body)
	getOutput.Body.Close()

	tassert.Errorf(t,
		*completeMultipartUpload.ETag == *getOutput.ETag,
		"ETag does not match between multipart upload and GET operation (%s != %s)",
		*completeMultipartUpload.ETag, *getOutput.ETag,
	)
}

// This tests checks that when there is no object locally in the AIStore, we
// won't get it from S3.
func TestDisableColdGet(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)

	bprops, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setBucketFeatures(t, bck, bprops, feat.S3PresignedRequest|feat.DisableColdGET)

	tools.SetClusterConfig(t, cos.StrKVs{"features": feat.S3ReverseProxy.String()})
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
	})

	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(false /*pathStyle*/), Region: env.AwsDefaultRegion()})

	putOutput, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   io.LimitReader(rand.Reader, fileSize),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, putOutput.ETag != nil, "ETag for PUT operation was not set")
	tassert.Errorf(t, *putOutput.ETag != "", "ETag for PUT operation is empty")

	err = api.EvictRemoteBucket(baseParams, bck, true)
	tassert.CheckFatal(t, err)

	_, err = s3Client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.Fatalf(t, err != nil, "Expected GET to fail %v", err)
}

// export AWS_PROFILE=default; export AIS_ENDPOINT="http://localhost:8080"; export BUCKET="aws://..."; go test -v -run="TestS3ETag" -count=1 ./ais/test/.
func TestS3ETag(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, Bck: cliBck, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
		objSize = 50 * cos.KiB
	)

	_, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		loadCredentials(t),
	)
	tassert.CheckFatal(t, err)
	s3Client := s3.NewFromConfig(cfg)

	t.Run("PutObject", func(t *testing.T) {
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(objSize), CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		oah, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)

		output, err := s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bck.Name),
			Key:    aws.String(objName),
		})
		tassert.CheckFatal(t, err)

		attrs := oah.Attrs()
		etag, _ := attrs.GetCustomKey(cmn.ETag)
		md5Hash, _ := attrs.GetCustomKey(cmn.MD5ObjMD)
		tassert.Errorf(t, etag == *output.ETag, "ETag for PUT does not match (local: %v != remote: %v)", etag, *output.ETag)
		tassert.Errorf(t, etag == `"`+md5Hash+`"`, "ETag must be equivalent to MD5 hash (etag: %v != md5: %v)", etag, md5Hash)
	})

	t.Run("PutObjectMultipart", func(t *testing.T) {
		const (
			objSize  = 20 * cos.MiB
			partSize = 5 * cos.MiB
		)

		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(objSize), CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		cfg.HTTPClient = newS3Client(true /*pathStyle*/)
		aisClient := s3.NewFromConfig(cfg)
		uploader := s3manager.NewUploader(aisClient, func(uploader *s3manager.Uploader) {
			uploader.PartSize = partSize
			uploader.ClientOptions = []func(*s3.Options){
				func(opts *s3.Options) {
					opts.BaseEndpoint = aws.String(proxyURL)
				},
			}
		}, func(uploader *s3manager.Uploader) {
			uploader.ClientOptions = append(uploader.ClientOptions, func(options *s3.Options) {
				options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
					return stack.Finalize.Add(&addGetBodyMiddleware{}, middleware.After)
				})
			})
		})
		multipartOutput, err := uploader.Upload(t.Context(), &s3.PutObjectInput{
			Bucket:        aws.String(bck.Name),
			Key:           aws.String(objName),
			Body:          reader,
			ContentLength: aws.Int64(objSize),
		})
		tassert.CheckFatal(t, err)

		headOutput, err := s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bck.Name),
			Key:    aws.String(objName),
		})
		tassert.CheckFatal(t, err)

		tassert.Errorf(t, *multipartOutput.ETag == *headOutput.ETag, "ETag for PUT does not match (local: %v != remote: %v)", *multipartOutput.ETag, *headOutput.ETag)
	})
}

// export AWS_PROFILE=default; export AIS_ENDPOINT="http://localhost:8080"; export BUCKET="aws://..."; go test -v -run="TestS3ObjMetadata" -count=1 ./ais/test/.
func TestS3ObjMetadata(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, Bck: cliBck, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
		objSize = 50 * cos.KiB
	)

	_, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		loadCredentials(t),
	)
	tassert.CheckFatal(t, err)
	s3Client := s3.NewFromConfig(cfg)

	metadata := map[string]string{
		"User": "guest",
		"name": "test",
	}

	t.Run("PutObject", func(t *testing.T) {
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(objSize), CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		header := make(http.Header)
		for k, v := range metadata {
			header.Set(aiss3.HeaderMetaPrefix+k, v)
		}
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     reader,
			Header:     header,
		})
		tassert.CheckFatal(t, err)

		output, err := s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bck.Name),
			Key:    aws.String(objName),
		})
		tassert.CheckFatal(t, err)

		for k, v := range metadata {
			tassert.Errorf(t, output.Metadata[strings.ToLower(k)] == v, `Metadata does not match (key: %q, local: %q != remote: %q)`, k, v, output.Metadata[k])
		}
	})

	t.Run("PutObjectMultipart", func(t *testing.T) {
		const (
			objSize  = 20 * cos.MiB
			partSize = 5 * cos.MiB
		)

		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(objSize), CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		cfg.HTTPClient = newS3Client(true /*pathStyle*/)
		aisClient := s3.NewFromConfig(cfg)
		uploader := s3manager.NewUploader(aisClient, func(uploader *s3manager.Uploader) {
			uploader.PartSize = partSize
			uploader.ClientOptions = []func(*s3.Options){
				func(opts *s3.Options) {
					opts.BaseEndpoint = aws.String(proxyURL)
				},
			}
		}, func(uploader *s3manager.Uploader) {
			uploader.ClientOptions = append(uploader.ClientOptions, func(options *s3.Options) {
				options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
					return stack.Finalize.Add(&addGetBodyMiddleware{}, middleware.After)
				})
			})
		})
		_, err = uploader.Upload(t.Context(), &s3.PutObjectInput{
			Bucket:        aws.String(bck.Name),
			Key:           aws.String(objName),
			Body:          reader,
			ContentLength: aws.Int64(objSize),
			Metadata:      metadata,
		})
		tassert.CheckFatal(t, err)

		headOutput, err := s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bck.Name),
			Key:    aws.String(objName),
		})
		tassert.CheckFatal(t, err)

		for k, v := range metadata {
			tassert.Errorf(t, headOutput.Metadata[strings.ToLower(k)] == v, `Metadata does not match (key: %q, local: %q != remote: %q)`, k, v, headOutput.Metadata[k])
		}
	})
}

// This test specifically targets the rlock implementation in tgts3mpt.go
// We will create large object (forces multipart storage) + concurrent S3 range requests
func TestS3MultipartPartOperations(t *testing.T) {
	setupS3Compat(t) // Enable S3 JWT compat if auth is enabled

	var (
		proxyURL = tools.GetPrimaryURL()
		bck      = cmn.Bck{Name: "test-mpt-rlock-" + trand.String(6), Provider: apc.AIS}
		objName  = "mpt-rlock-test.dat"
		objSize  = int64(200 * cos.MiB) // Large object for multipart storage
	)

	// Create AIStore bucket
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: tools.BaseAPIParams(proxyURL),
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
	})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Created large object %s (%s)", objName, cos.SizeIEC(objSize))

	const (
		numConcurrentReaders   = 8
		numIterationsPerReader = 3
	)
	// Use S3 client for reading
	var (
		s3Client = s3.New(s3.Options{
			HTTPClient:   newS3Client(true /*pathStyle*/),
			Region:       "us-east-1",
			BaseEndpoint: aws.String(proxyURL),
			UsePathStyle: true,
			Credentials:  getS3Credentials(t),
		})
		errorsCh = make(chan error, numConcurrentReaders*numIterationsPerReader*2)
		wg       sync.WaitGroup
	)
	tlog.Logfln("Starting concurrent rlock test on multipart object: %d readers × %d iterations × 2 operations = %d total operations",
		numConcurrentReaders, numIterationsPerReader, numConcurrentReaders*numIterationsPerReader*2)

	// Test concurrent operations on the large object
	for readerID := range numConcurrentReaders {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for iteration := range numIterationsPerReader {
				// Range requests across different parts of large object
				startByte := int64(id*25*cos.MiB + iteration*cos.MiB)
				endByte := startByte + 1023 // 1KB chunks

				getOutput, err := s3Client.GetObject(t.Context(), &s3.GetObjectInput{
					Bucket: aws.String(bck.Name),
					Key:    aws.String(objName),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, endByte)),
				})
				if err != nil {
					errorsCh <- fmt.Errorf("reader %d iteration %d GetObject range failed: %v", id, iteration, err)
					return
				}

				data, err := io.ReadAll(getOutput.Body)
				getOutput.Body.Close()
				if err != nil {
					errorsCh <- fmt.Errorf("reader %d iteration %d: failed to read range body: %v", id, iteration, err)
					return
				}

				if len(data) != 1024 {
					errorsCh <- fmt.Errorf("reader %d iteration %d: expected 1024 bytes, got %d", id, iteration, len(data))
					return
				}

				// Head requests
				_, err = s3Client.HeadObject(t.Context(), &s3.HeadObjectInput{
					Bucket: aws.String(bck.Name),
					Key:    aws.String(objName),
				})
				if err != nil {
					errorsCh <- fmt.Errorf("reader %d iteration %d HeadObject failed: %v", id, iteration, err)
					return
				}
			}
		}(readerID)
	}

	// Wait for all concurrent operations to complete
	wg.Wait()
	close(errorsCh)

	// Check for any race condition errorsCh
	for err := range errorsCh {
		tassert.CheckFatal(t, err)
	}
}

func TestS3MultipartErrorHandling(t *testing.T) {
	setupS3Compat(t) // Enable S3 JWT compat if auth is enabled

	var (
		proxyURL = tools.GetPrimaryURL()
		bck      = cmn.Bck{Name: "test-s3-mpt-err-" + trand.String(6), Provider: apc.AIS}
		testData = "test"
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tools.SetClusterConfig(t, cos.StrKVs{"features": feat.S3ReverseProxy.String()})
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"features": "0"})
	})

	s3Client := s3.New(s3.Options{
		HTTPClient:   newS3Client(true /*pathStyle*/),
		Region:       env.AwsDefaultRegion(),
		BaseEndpoint: aws.String(proxyURL),
		UsePathStyle: true,
		Credentials:  getS3Credentials(t),
	})

	// Call UploadPart with invalid upload ID
	_, err := s3Client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:        aws.String(bck.Name),
		Key:           aws.String("test-object"),
		PartNumber:    aws.Int32(1),
		UploadId:      aws.String("invalid-upload-id"),
		Body:          strings.NewReader(testData),
		ContentLength: aws.Int64(int64(len(testData))),
	})

	// Check that the error code is NoSuchUpload
	var s3Err smithy.APIError
	tassert.Errorf(t, errors.As(err, &s3Err) && s3Err.ErrorCode() == "NoSuchUpload",
		"expected NoSuchUpload error, got: %v", err)

	// Check that the status code is 404
	var httpErr *smithyhttp.ResponseError
	tassert.Errorf(t, errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == 404,
		"expected HTTP 404, got: %v", err)
}

func TestS3JWTAuth(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})
	setupS3Compat(t) // Enable S3 JWT compat mode

	var (
		proxyURL = tools.GetPrimaryURL()
		bck      = cmn.Bck{Name: "test-s3-jwt-" + trand.String(6), Provider: apc.AIS}
		objName  = "test-object.txt"
	)

	// Get authenticated BaseParams
	authBP := tools.BaseAPIParams()

	// Create test bucket
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// VERIFY: S3 JWT compat is enabled
	updatedConfig, err := api.GetClusterConfig(authBP)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, updatedConfig.Auth.AllowS3TokenCompat,
		"S3 JWT compat mode should be enabled but got: %v", updatedConfig.Auth.AllowS3TokenCompat)
	tlog.Logfln("✓ S3 JWT compatibility mode enabled")

	// Get a valid JWT token from the authenticated BaseParams
	testJWT := authBP.Token
	tassert.Fatalf(t, testJWT != "", "Expected valid auth token from tools.BaseAPIParams()")
	tlog.Logfln("Using JWT token[%s...] (length: %d bytes) for S3 auth", testJWT[:min(16, len(testJWT))], len(testJWT))

	// Create AWS SDK client with JWT as SessionToken
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(getS3Credentials(t)),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)

	cfg.HTTPClient = newS3Client(false /*pathStyle*/)
	cfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	s3Client := s3.NewFromConfig(cfg)

	// Test 1: List buckets
	// This request will have:
	// - Authorization: AWS4-HMAC-SHA256 ... (SigV4 signature)
	// - X-Amz-Security-Token: <JWT>
	// AIStore should ignore SigV4 and authenticate using JWT!
	tlog.Logln("Testing ListBuckets with JWT via X-Amz-Security-Token...")
	listOutput, err := s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Successfully listed %d buckets using JWT auth", len(listOutput.Buckets))

	// Verify our test bucket is in the list
	found := false
	for _, b := range listOutput.Buckets {
		if *b.Name == bck.Name {
			found = true
			break
		}
	}
	tassert.Errorf(t, found, "Expected to find bucket %s in list", bck.Name)

	// Test 2: PUT object with JWT auth
	tlog.Logfln("Testing PutObject to %s with JWT auth...", bck.Cname(objName))
	testData := "test data for JWT S3 auth"
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   strings.NewReader(testData),
	})
	tassert.CheckFatal(t, err)
	tlog.Logln("Successfully PUT object using JWT auth")

	// Test 3: GET object with JWT auth
	tlog.Logfln("Testing GetObject from %s with JWT auth...", bck.Cname(objName))
	getOutput, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.CheckFatal(t, err)
	defer getOutput.Body.Close()

	body, err := io.ReadAll(getOutput.Body)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(body) == testData, "Expected data %q, got %q", testData, string(body))
	tlog.Logln("Successfully GET object using JWT auth")

	// Test 4: NEGATIVE TEST - Verify request WITHOUT JWT fails (proves auth is enforced)
	tlog.Logln("Testing that unauthenticated request fails (negative test)...")
	unauthCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"dummy-key",
				"dummy-secret",
				"", // No SessionToken = no JWT
			),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)

	unauthCfg.HTTPClient = newS3Client(false)
	unauthCfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	unauthClient := s3.NewFromConfig(unauthCfg)

	_, err = unauthClient.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected unauthenticated request to fail, but it succeeded")
	tlog.Logfln("✓ Unauthenticated request failed as expected: %v", err)
}

// TestS3JWTAuthFailures validates that S3 requests properly fail when S3 JWT compat mode
// is enabled but invalid or missing credentials are provided
func TestS3JWTAuthFailures(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})
	setupS3Compat(t) // Enable S3 JWT compat mode

	var (
		proxyURL = tools.GetPrimaryURL()
		bck      = cmn.Bck{Name: "test-s3-jwt-fail-" + trand.String(6), Provider: apc.AIS}
	)

	// Get authenticated BaseParams
	authBP := tools.BaseAPIParams()

	// Create test bucket
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// VERIFY: S3 JWT compat is enabled
	updatedConfig, err := api.GetClusterConfig(authBP)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, updatedConfig.Auth.AllowS3TokenCompat,
		"S3 JWT compat mode should be enabled but got: %v", updatedConfig.Auth.AllowS3TokenCompat)
	tlog.Logfln("✓ S3 JWT compatibility mode enabled")

	// Test 1: Request with NO credentials at all
	tlog.Logln("Test 1: Request with NO credentials should fail...")
	noCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("", "", ""),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)
	noCfg.HTTPClient = newS3Client(false)
	noCfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	noCredsClient := s3.NewFromConfig(noCfg)

	_, err = noCredsClient.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected request with no credentials to fail, but it succeeded")
	tlog.Logfln("✓ Request with no credentials failed as expected: %v", err)

	// Test 2: Request with AWS SigV4 credentials but NO JWT (SessionToken empty)
	tlog.Logln("Test 2: Request with SigV4 credentials but NO JWT should fail...")
	sigv4Cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"AKIAIOSFODNN7EXAMPLE",                     // Valid-looking AWS access key
				"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", // Valid-looking secret
				"", // No SessionToken = no JWT
			),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)
	sigv4Cfg.HTTPClient = newS3Client(false)
	sigv4Cfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	sigv4Client := s3.NewFromConfig(sigv4Cfg)

	_, err = sigv4Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected SigV4-only request to fail, but it succeeded")
	tlog.Logfln("✓ SigV4-only request (no JWT) failed as expected: %v", err)

	// Test 3: Request with INVALID JWT format
	tlog.Logln("Test 3: Request with invalid JWT format should fail...")
	invalidJWTCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"dummy-key",
				"dummy-secret",
				"not-a-valid-jwt-token", // Invalid JWT format
			),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)
	invalidJWTCfg.HTTPClient = newS3Client(false)
	invalidJWTCfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	invalidJWTClient := s3.NewFromConfig(invalidJWTCfg)

	_, err = invalidJWTClient.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected request with invalid JWT to fail, but it succeeded")
	tlog.Logfln("✓ Invalid JWT format failed as expected: %v", err)

	// Test 4: Request with malformed JWT (looks like JWT but invalid signature)
	tlog.Logln("Test 4: Request with malformed JWT signature should fail...")
	malformedJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJoYWNrZXIiLCJhZG1pbiI6dHJ1ZSwiZXhwIjo5OTk5OTk5OTk5fQ.InvalidSignature"
	malformedCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"dummy-key",
				"dummy-secret",
				malformedJWT,
			),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)
	malformedCfg.HTTPClient = newS3Client(false)
	malformedCfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	malformedClient := s3.NewFromConfig(malformedCfg)

	_, err = malformedClient.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected request with malformed JWT to fail, but it succeeded")
	tlog.Logfln("✓ Malformed JWT signature failed as expected: %v", err)
}

// TestS3JWTAuthDisabledByDefault validates backward compatibility:
// When auth is enabled but allow_s3_token_compat is false (default),
// valid JWT tokens in X-Amz-Security-Token should be rejected
func TestS3JWTAuthDisabledByDefault(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresAuth: true})

	var (
		proxyURL = tools.GetPrimaryURL()
		bck      = cmn.Bck{Name: "test-s3-jwt-disabled-" + trand.String(6), Provider: apc.AIS}
	)

	// Get authenticated BaseParams
	authBP := tools.BaseAPIParams()

	// Get current config
	clusterConfig, err := api.GetClusterConfig(authBP)
	tassert.CheckFatal(t, err)
	originalFeatures := clusterConfig.Features.String()
	originalS3TokenCompat := strconv.FormatBool(clusterConfig.Auth.AllowS3TokenCompat)

	// Create test bucket
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Enable S3 reverse proxy but keep allow_s3_token_compat disabled (false)
	tlog.Logln("Enabling S3 reverse proxy with JWT auth mode DISABLED...")
	tools.SetClusterConfig(t, cos.StrKVs{
		"features":                   feat.S3ReverseProxy.String(),
		"auth.allow_s3_token_compat": "false",
	})
	t.Cleanup(func() {
		// Restore original config values
		tools.SetClusterConfig(t, cos.StrKVs{
			"features":                   originalFeatures,
			"auth.allow_s3_token_compat": originalS3TokenCompat,
		})
	})

	// VERIFY: S3 JWT compat is disabled
	updatedConfig, err := api.GetClusterConfig(authBP)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !updatedConfig.Auth.AllowS3TokenCompat,
		"S3 JWT compat mode should be disabled but got: %v", updatedConfig.Auth.AllowS3TokenCompat)
	tlog.Logfln("✓ S3 JWT compatibility mode is disabled (backward compatibility mode)")

	// Get a valid JWT token
	testJWT := authBP.Token
	tassert.Fatalf(t, testJWT != "", "Expected valid auth token from tools.BaseAPIParams()")
	tlog.Logfln("Attempting S3 request with valid JWT token (length: %d bytes)", len(testJWT))

	// Create AWS SDK client with JWT as SessionToken
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"dummy-access-key",
				"dummy-secret-key",
				testJWT, // Valid JWT in X-Amz-Security-Token header
			),
		),
		config.WithRegion(env.AwsDefaultRegion()),
	)
	tassert.CheckFatal(t, err)

	cfg.HTTPClient = newS3Client(false)
	cfg.BaseEndpoint = aws.String(proxyURL + "/s3")
	s3Client := s3.NewFromConfig(cfg)

	// Attempt S3 request - should FAIL because allow_s3_token_compat is disabled
	tlog.Logln("Testing that valid JWT is rejected when feature is disabled...")
	_, err = s3Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	tassert.Fatalf(t, err != nil, "Expected request to fail when allow_s3_token_compat=false, but it succeeded")
	tlog.Logfln("✓ Valid JWT was correctly rejected (backward compatibility preserved): %v", err)
}
