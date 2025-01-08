// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		context.Background(),
		config.WithSharedConfigProfile("default"),
	)
	tassert.CheckFatal(t, err)
	cfg.HTTPClient = newS3Client()
	s3Client := s3.NewFromConfig(cfg)
	*/
	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(false /*pathStyle*/), Region: env.AwsDefaultRegion()})

	putOutput, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   io.LimitReader(rand.Reader, fileSize),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, putOutput.ETag != nil, "ETag for PUT operation was not set")
	tassert.Errorf(t, *putOutput.ETag != "", "ETag for PUT operation is empty")

	getOutput, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

	createMultipartUploadOutput, err := s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, createMultipartUploadOutput.UploadId != nil, "UploadId for CreateMultipartUpload operation was not set")
	tassert.Errorf(t, *createMultipartUploadOutput.UploadId != "", "UploadId for CreateMultipartUpload operation is empty")

	var parts []types.CompletedPart //nolint:prealloc // Not needed.
	for i := 1; i <= 3; i++ {
		uploadPartOutput, err := s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
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

	completeMultipartUpload, err := s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bck.Name),
		Key:             aws.String(objName),
		UploadId:        createMultipartUploadOutput.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, completeMultipartUpload.ETag != nil, "ETag for CreateMultipartUpload was not set")

	getOutput, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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

	putOutput, err := s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bck.Name),
		Key:    aws.String(objName),
		Body:   io.LimitReader(rand.Reader, fileSize),
	})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, putOutput.ETag != nil, "ETag for PUT operation was not set")
	tassert.Errorf(t, *putOutput.ETag != "", "ETag for PUT operation is empty")

	err = api.EvictRemoteBucket(baseParams, bck, true)
	tassert.CheckFatal(t, err)

	_, err = s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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
		context.Background(),
		loadCredentials(t),
	)
	tassert.CheckFatal(t, err)
	s3Client := s3.NewFromConfig(cfg)

	t.Run("PutObject", func(t *testing.T) {
		reader, err := readers.NewRand(int64(objSize), cos.ChecksumNone)
		tassert.CheckFatal(t, err)
		oah, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     reader,
		})
		tassert.CheckFatal(t, err)

		output, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
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

		reader, err := readers.NewRand(int64(objSize), cos.ChecksumNone)
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
		multipartOutput, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket:        aws.String(bck.Name),
			Key:           aws.String(objName),
			Body:          reader,
			ContentLength: aws.Int64(objSize),
		})
		tassert.CheckFatal(t, err)

		headOutput, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bck.Name),
			Key:    aws.String(objName),
		})
		tassert.CheckFatal(t, err)

		tassert.Errorf(t, *multipartOutput.ETag == *headOutput.ETag, "ETag for PUT does not match (local: %v != remote: %v)", *multipartOutput.ETag, *headOutput.ETag)
	})
}
