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
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
}

func checkOCIWritePrecondition(t *testing.T, err error) {
	t.Helper()
	var response *smithyhttp.ResponseError
	var apiErr smithy.APIError
	tassert.Fatalf(t, errors.As(err, &response) && response.HTTPStatusCode() == http.StatusPreconditionFailed, "expected HTTP 412, got %v", err)
	tassert.Fatalf(t, errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed", "expected S3 PreconditionFailed, got %v", err)
}

// enforce native PUT predicates even when identical checksums would skip an upload.
func TestNativeOCIConditionalWrites(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, RequiredCloudProvider: apc.OCI})
	config, err := api.GetClusterConfig(baseParams)
	tassert.CheckFatal(t, err)
	if config.Features.IsSet(feat.SkipVC) {
		t.Skip("requires checksum comparison enabled to exercise the no-op path")
	}
	props, err := api.HeadBucket(baseParams, cliBck, false)
	tassert.CheckFatal(t, err)
	_, err = api.SetBucketProps(baseParams, cliBck, &cmn.BpropsToSet{Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumMD5)}})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, cliBck, &cmn.BpropsToSet{Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(props.Cksum.Type)}})
		tassert.CheckError(t, err)
	})
	prefix := "conditional-native-" + trand.String(12) + "/"
	object := prefix + "identical"
	t.Logf("isolated object prefix: %s", prefix)
	t.Cleanup(func() { tassert.CheckError(t, api.DeleteObject(baseParams, cliBck, object)) })
	data := []byte("native conditional write")
	_, checksum, err := cos.ChecksumReader(bytes.NewReader(data), cos.ChecksumMD5)
	tassert.CheckFatal(t, err)
	put := func(header, value string) error {
		headers := make(http.Header)
		if header != "" {
			headers.Set(header, value)
		}
		_, err := api.PutObject(&api.PutArgs{
			Context: t.Context(), BaseParams: baseParams, Bck: cliBck, ObjName: object,
			Reader: cos.NewByteReader(data), Size: uint64(len(data)), Cksum: &checksum.Cksum, Header: headers,
		})
		return err
	}
	tassert.CheckFatal(t, put("If-None-Match", "*"))
	var output bytes.Buffer
	read, err := api.GetObject(baseParams, cliBck, object, &api.GetArgs{Writer: &output})
	tassert.CheckFatal(t, err)
	attrs := read.Attrs()
	tassert.Fatalf(t, checksum.Equal(attrs.Checksum()), "requires matching checksum: stored %v, request %v", attrs.Checksum(), &checksum.Cksum)
	for _, tc := range []struct {
		name, header, value string
		status              int
	}{
		{"create-only", "If-None-Match", "*", http.StatusPreconditionFailed},
		{"stale-etag", "If-Match", `"stale"`, http.StatusPreconditionFailed},
		{"invalid-predicate", "If-None-Match", `"etag"`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := put(tc.header, tc.value)
			response := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, response != nil && response.Status == tc.status, "expected HTTP %d, got %v", tc.status, err)
		})
	}
	// A valid write predicate must not disable end-to-end body validation.
	for _, cksumType := range []string{cos.ChecksumMD5, cos.ChecksumOneXxh} {
		for _, replace := range []bool{false, true} {
			t.Run(fmt.Sprintf("checksum-%s/replace-%t", cksumType, replace), func(t *testing.T) {
				name := prefix + fmt.Sprintf("checksum-%s-%t", cksumType, replace)
				t.Cleanup(func() {
					if err := api.DeleteObject(baseParams, cliBck, name); !cmn.IsErrHTTPNotFound(err) {
						tassert.CheckError(t, err)
					}
				})
				headers := make(http.Header)
				headers.Set("If-None-Match", "*")
				if replace {
					_, err := api.PutObject(&api.PutArgs{
						Context: t.Context(), BaseParams: baseParams, Bck: cliBck, ObjName: name,
						Reader: cos.NewByteReader(data), Size: uint64(len(data)), Cksum: &checksum.Cksum,
					})
					tassert.CheckFatal(t, err)
					head, err := api.HeadObjectV2(baseParams, cliBck, name, apc.GetPropsCustom, api.HeadArgs{FltPresence: apc.FltPresent})
					tassert.CheckFatal(t, err)
					etag, ok := head.ObjAttrs.GetCustomKey(cmn.ETag)
					tassert.Fatalf(t, ok && etag != "", "missing remote ETag")
					headers.Del("If-None-Match")
					headers.Set("If-Match", cmn.QuoteETag(etag))
				}
				// The expected checksum describes the old content, not this body.
				_, expected, err := cos.ChecksumReader(bytes.NewReader(data), cksumType)
				tassert.CheckFatal(t, err)
				changed := []byte("different native conditional write")
				_, err = api.PutObject(&api.PutArgs{
					Context: t.Context(), BaseParams: baseParams, Bck: cliBck, ObjName: name,
					Reader: cos.NewByteReader(changed), Size: uint64(len(changed)), Cksum: &expected.Cksum, Header: headers,
				})
				tassert.Fatalf(t, cos.IsErrBadCksum(err), "expected checksum rejection, got %v", err)
				if replace {
					tassert.CheckFatal(t, api.EvictObject(baseParams, cliBck, name))
					var got bytes.Buffer
					_, err := api.GetObject(baseParams, cliBck, name, &api.GetArgs{Writer: &got})
					tassert.CheckFatal(t, err)
					tassert.Fatalf(t, bytes.Equal(got.Bytes(), data), "bad checksum overwrote remote bytes")
				} else {
					_, err := api.HeadObjectV2(baseParams, cliBck, name, apc.GetPropsName, api.HeadArgs{FltPresence: apc.FltExists})
					tassert.Fatalf(t, cmn.IsErrHTTPNotFound(err), "bad checksum created a remote object: %v", err)
				}
				// The same predicate accepts a body with its correct checksum.
				_, correct, err := cos.ChecksumReader(bytes.NewReader(changed), cksumType)
				tassert.CheckFatal(t, err)
				_, err = api.PutObject(&api.PutArgs{
					Context: t.Context(), BaseParams: baseParams, Bck: cliBck, ObjName: name,
					Reader: cos.NewByteReader(changed), Size: uint64(len(changed)), Cksum: &correct.Cksum, Header: headers,
				})
				tassert.CheckFatal(t, err)
				tassert.CheckFatal(t, api.EvictObject(baseParams, cliBck, name))
				var got bytes.Buffer
				_, err = api.GetObject(baseParams, cliBck, name, &api.GetArgs{Writer: &got})
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(got.Bytes(), changed), "valid conditional write lost remote bytes")
			})
		}
	}
	tassert.CheckFatal(t, put("", ""))
	head, err := api.HeadObjectV2(baseParams, cliBck, object, apc.GetPropsCustom, api.HeadArgs{FltPresence: apc.FltPresent})
	tassert.CheckFatal(t, err)
	beforeETag, _ := attrs.GetCustomKey(cmn.ETag)
	afterETag, _ := head.ObjAttrs.GetCustomKey(cmn.ETag)
	tassert.Fatalf(t, beforeETag != "" && beforeETag == afterETag, "unconditional identical PUT reached OCI instead of taking the shortcut")
	tassert.CheckFatal(t, api.EvictObject(baseParams, cliBck, object))
	output.Reset()
	_, err = api.GetObject(baseParams, cliBck, object, &api.GetArgs{Writer: &output})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(output.Bytes(), data), "rejected native PUT changed remote bytes")
}
