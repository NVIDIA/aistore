// Package integration_test.
// no-copyright
package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// exercise conditions through the S3 frontend and native OCI, including cold reads.
func TestS3OCIConditionalWrites(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, RequiredCloudProvider: apc.OCI})
	_, err := api.HeadBucket(baseParams, cliBck, false)
	tassert.CheckFatal(t, err)
	tools.EnableClusterFeatures(t, feat.S3ReverseProxy)
	client := s3.New(s3.Options{
		HTTPClient: newS3Client(true), Region: env.AwsDefaultRegion(),
		BaseEndpoint: aws.String(proxyURL), UsePathStyle: true, Credentials: getS3Credentials(t),
	}, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(&addGetBodyMiddleware{}, middleware.After)
		})
	})
	prefix := "conditional-oci-" + trand.String(12) + "/"
	t.Logf("isolated object prefix: %s", prefix)
	key := func(name string) *string {
		object := prefix + name
		t.Cleanup(func() { tassert.CheckError(t, api.DeleteObject(baseParams, cliBck, object)) })
		return &object
	}
	put := func(object *string, data []byte, match, none *string) (*s3.PutObjectOutput, error) {
		return client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &cliBck.Name, Key: object, Body: bytes.NewReader(data), IfMatch: match, IfNoneMatch: none,
		})
	}
	coldRead := func(object *string, want []byte) {
		t.Helper()
		if err := api.EvictObject(baseParams, cliBck, *object); !cmn.IsErrHTTPNotFound(err) {
			tassert.CheckFatal(t, err)
		}
		out, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: &cliBck.Name, Key: object})
		tassert.CheckFatal(t, err)
		got, readErr := io.ReadAll(out.Body)
		closeErr := out.Body.Close()
		tassert.CheckFatal(t, readErr)
		tassert.CheckFatal(t, closeErr)
		tassert.Fatalf(t, bytes.Equal(got, want), "cold read differs from winning write")
	}
	for _, size := range []int{8, 128 * 1024} {
		t.Run(fmt.Sprintf("put-%d", size), func(t *testing.T) {
			object := key(fmt.Sprintf("put-%d", size))
			first := bytes.Repeat([]byte("a"), size)
			original, err := put(object, first, nil, aws.String("*"))
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, aws.ToString(original.ETag) != "", "missing ETag")
			// Identical bytes must still reach OCI and fail the create-only condition.
			_, err = put(object, first, nil, aws.String("*"))
			checkOCIWritePrecondition(t, err)
			_, err = put(object, []byte("rejected"), aws.String(`"stale"`), nil)
			checkOCIWritePrecondition(t, err)
			updated := bytes.Repeat([]byte("b"), size)
			_, err = put(object, updated, original.ETag, nil)
			tassert.CheckFatal(t, err)
			_, err = put(object, first, original.ETag, nil)
			checkOCIWritePrecondition(t, err)
			coldRead(object, updated)
		})
	}
	t.Run("multipart", func(t *testing.T) {
		object := key("multipart")
		complete := func(data []byte, match, none *string) (*s3.CompleteMultipartUploadOutput, error) {
			create, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{Bucket: &cliBck.Name, Key: object})
			tassert.CheckFatal(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
				defer cancel()
				_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId})
			})
			part, err := client.UploadPart(t.Context(), &s3.UploadPartInput{Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(data)})
			tassert.CheckFatal(t, err)
			return client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
				Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId, IfMatch: match, IfNoneMatch: none,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}},
			})
		}
		original, err := complete([]byte("multipart original"), nil, aws.String("*"))
		tassert.CheckFatal(t, err)
		_, err = complete([]byte("rejected"), nil, aws.String("*"))
		checkOCIWritePrecondition(t, err)
		_, err = complete([]byte("rejected"), aws.String(`"stale"`), nil)
		checkOCIWritePrecondition(t, err)
		updated := []byte("multipart updated")
		_, err = complete(updated, original.ETag, nil)
		tassert.CheckFatal(t, err)
		coldRead(object, updated)
	})
	t.Run("multipart-races", func(t *testing.T) {
		for round := range 5 {
			object := key(fmt.Sprintf("multipart-race-%d", round))
			var etag *string
			for _, mode := range []string{"create", "replace"} {
				inputs := make([]s3.CompleteMultipartUploadInput, 8)
				payloads := make([][]byte, len(inputs))
				for i := range inputs {
					create, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{Bucket: &cliBck.Name, Key: object})
					tassert.CheckFatal(t, err)
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
						defer cancel()
						_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId})
					})
					payloads[i] = fmt.Appendf(nil, "%s-%d", mode, i)
					part, err := client.UploadPart(t.Context(), &s3.UploadPartInput{Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(payloads[i])})
					tassert.CheckFatal(t, err)
					inputs[i] = s3.CompleteMultipartUploadInput{
						Bucket: &cliBck.Name, Key: object, UploadId: create.UploadId, IfMatch: etag,
						MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}},
					}
					if mode == "create" {
						inputs[i].IfNoneMatch = aws.String("*")
					}
				}
				type outcome struct {
					output *s3.CompleteMultipartUploadOutput
					err    error
					index  int
				}
				results := make(chan outcome, len(inputs))
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range inputs {
					wg.Go(func() {
						<-start
						out, err := client.CompleteMultipartUpload(t.Context(), &inputs[i])
						results <- outcome{out, err, i}
					})
				}
				close(start)
				wg.Wait()
				close(results)
				winners := 0
				var winner []byte
				var next *string
				for result := range results {
					if result.err != nil {
						checkOCIWriteConflict(t, result.err)
						continue
					}
					winners++
					next, winner = result.output.ETag, payloads[result.index]
				}
				tassert.Fatalf(t, winners == 1, "multipart %s: expected one winner, got %d", mode, winners)
				coldRead(object, winner)
				etag = next
			}
		}
	})
	t.Run("races", func(t *testing.T) {
		for round := range 5 {
			object := key(fmt.Sprintf("race-%d", round))
			var etag *string
			for _, mode := range []string{"create", "replace"} {
				type outcome struct {
					output *s3.PutObjectOutput
					err    error
					data   []byte
				}
				results := make(chan outcome, 8)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range 8 {
					wg.Go(func() {
						<-start
						data := fmt.Appendf(nil, "%s-%d", mode, i)
						var none *string
						if mode == "create" {
							none = aws.String("*")
						}
						out, err := put(object, data, etag, none)
						results <- outcome{out, err, data}
					})
				}
				close(start)
				wg.Wait()
				close(results)
				winners := 0
				var winner []byte
				var next *string
				for result := range results {
					if result.err != nil {
						checkOCIWriteConflict(t, result.err)
						continue
					}
					winners++
					next = result.output.ETag
					winner = result.data
				}
				tassert.Fatalf(t, winners == 1, "%s: expected one winner, got %d", mode, winners)
				coldRead(object, winner)
				etag = next
			}
		}
	})
}

func checkOCIWritePrecondition(t *testing.T, err error) {
	t.Helper()
	var response *smithyhttp.ResponseError
	var apiErr smithy.APIError
	tassert.Fatalf(t, errors.As(err, &response) && response.HTTPStatusCode() == http.StatusPreconditionFailed, "expected HTTP 412, got %v", err)
	tassert.Fatalf(t, errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed", "expected S3 PreconditionFailed, got %v", err)
}

// competing writes may return a retryable S3 conflict rather than a failed predicate.
func checkOCIWriteConflict(t *testing.T, err error) {
	t.Helper()
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) && response.HTTPStatusCode() == http.StatusConflict {
		var apiErr smithy.APIError
		tassert.Fatalf(t, errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConditionalRequestConflict", "expected S3 ConditionalRequestConflict, got %v", err)
		return
	}
	checkOCIWritePrecondition(t, err)
}
