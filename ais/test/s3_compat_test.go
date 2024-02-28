// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type customTransport struct {
	rt http.RoundTripper
}

func (t *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	bucket := strings.Split(req.URL.Host, ".")[0]
	u, _ := url.Parse(proxyURL)
	req.URL.Host = u.Host
	req.URL.Path = "/s3/" + bucket + req.URL.Path
	return t.rt.RoundTrip(req)
}

func newCustomTransport() *customTransport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &customTransport{
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

func newS3Client() *http.Client {
	return &http.Client{
		Transport: newCustomTransport(),
	}
}

func setPresignedS3(t *testing.T, bck cmn.Bck) {
	f := feat.PresignedS3Req
	props := &cmn.BpropsToSet{Features: &f}
	_, err := api.SetBucketProps(baseParams, bck, props)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		var f feat.Flags
		props := &cmn.BpropsToSet{Features: &f}
		_, err := api.SetBucketProps(baseParams, bck, props)
		tassert.CheckFatal(t, err)
	})
}

func TestS3PassThroughPutGet(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)
	_, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setPresignedS3(t, bck)

	/* TODO -- FIXME: alternatively, use env vars AWS_PROFILE et al:
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithSharedConfigProfile("default"),
	)
	tassert.CheckFatal(t, err)
	cfg.HTTPClient = newS3Client()
	s3Client := s3.NewFromConfig(cfg)
	*/
	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(), Region: cmn.AwsDefaultRegion})

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

func TestS3PassThroughMultipart(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)
	_, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setPresignedS3(t, bck)

	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(), Region: cmn.AwsDefaultRegion})

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

// FIXME: This test should be enabled once implementation is fixed.
// This tests checks that when there is no object locally in the AIStore, we
// won't get it from S3.
func TestWriteThroughCacheNoColdGet(t *testing.T) {
	t.Skip()
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiresTLS: true, RequiredCloudProvider: apc.AWS})

	var (
		bck     = cliBck
		objName = "object.txt"
	)
	_, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	setPresignedS3(t, bck)

	s3Client := s3.New(s3.Options{HTTPClient: newS3Client(), Region: cmn.AwsDefaultRegion})

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
