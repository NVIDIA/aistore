//go:build oci

// Package integration_test.
// no-copyright
package integration_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
	"github.com/aws/smithy-go/middleware"
	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
	ociauth "github.com/oracle/oci-go-sdk/v65/common/auth"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// verify conditional automatic MPU rejection leaves no internal upload in OCI.
func TestS3OCIAutomaticMultipartCleanup(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, RequiredCloudProvider: apc.OCI})
	value := os.Getenv(env.OCIMultiPartUploadThreshold)
	if value == "" {
		t.Skipf("set %s in deployment and tests to exercise automatic MPU", env.OCIMultiPartUploadThreshold)
	}
	threshold, err := cos.ParseSize(value, "")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, threshold > 0 && threshold < 8*cos.MiB, "configure the automatic MPU threshold below 8MiB in deployment and tests")
	data := bytes.Repeat([]byte("a"), int(threshold)+1)
	t.Logf("automatic MPU: PUT size %d exceeds configured threshold %d", len(data), threshold)

	native := newOCIConditionalTestClient(t)
	namespace, err := native.GetNamespace(t.Context(), ocios.GetNamespaceRequest{})
	tassert.CheckFatal(t, err)
	prefix := "conditional-cleanup-" + trand.String(12) + "/"
	object := prefix + "object"
	t.Logf("isolated object prefix: %s", prefix)
	list := func(ctx context.Context) ([]ocios.MultipartUpload, error) {
		var uploads []ocios.MultipartUpload
		req := ocios.ListMultipartUploadsRequest{NamespaceName: namespace.Value, BucketName: &cliBck.Name}
		for {
			response, err := native.ListMultipartUploads(ctx, req)
			if err != nil {
				return uploads, err
			}
			for _, upload := range response.Items {
				if upload.Object != nil && *upload.Object == object {
					uploads = append(uploads, upload)
				}
			}
			if response.OpcNextPage == nil || *response.OpcNextPage == "" {
				return uploads, nil
			}
			req.Page = response.OpcNextPage
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		uploads, err := list(ctx)
		tassert.CheckError(t, err)
		if len(uploads) != 0 {
			t.Logf("fixture cleanup: aborting %d test-owned uploads after assertions", len(uploads))
		}
		for _, upload := range uploads {
			_, err := native.AbortMultipartUpload(ctx, ocios.AbortMultipartUploadRequest{
				NamespaceName: namespace.Value, BucketName: &cliBck.Name, ObjectName: upload.Object, UploadId: upload.UploadId,
			})
			tassert.CheckError(t, err)
		}
		if err := api.DeleteObject(baseParams, cliBck, object); !cmn.IsErrHTTPNotFound(err) {
			tassert.CheckError(t, err)
		}
	})
	_, err = api.HeadBucket(baseParams, cliBck, false)
	tassert.CheckFatal(t, err)
	tools.EnableClusterFeatures(t, feat.S3ReverseProxy)
	client := s3.New(s3.Options{
		HTTPClient: newS3Client(true), Region: env.AwsDefaultRegion(), BaseEndpoint: aws.String(proxyURL),
		UsePathStyle: true, Credentials: getS3Credentials(t),
	}, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(&addGetBodyMiddleware{}, middleware.After)
		})
	})
	put := func(match, none *string) error {
		_, err := client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &cliBck.Name, Key: &object, Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
			IfMatch: match, IfNoneMatch: none,
		})
		return err
	}
	tassert.CheckFatal(t, put(nil, aws.String("*")))
	head, err := native.HeadObject(t.Context(), ocios.HeadObjectRequest{
		NamespaceName: namespace.Value, BucketName: &cliBck.Name, ObjectName: &object,
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, head.OpcMultipartMd5 != nil && *head.OpcMultipartMd5 != "", "PUT did not use OCI multipart upload; match deployment and test thresholds")
	for _, name := range []string{"create-only", "stale-etag"} {
		t.Run(name, func(t *testing.T) {
			if name == "create-only" {
				checkOCIWritePrecondition(t, put(nil, aws.String("*")))
			} else {
				checkOCIWritePrecondition(t, put(aws.String(`"stale"`), nil))
			}
			uploads, err := list(t.Context())
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, len(uploads) == 0, "rejected automatic multipart completion left %d native OCI uploads", len(uploads))
		})
	}
	tassert.CheckFatal(t, api.EvictObject(baseParams, cliBck, object))
	var output bytes.Buffer
	_, err = api.GetObject(baseParams, cliBck, object, &api.GetArgs{Writer: &output})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(output.Bytes(), data), "rejected PUT or cleanup changed the committed object")
}

// use native OCI credentials to inspect uploads hidden from the S3 caller.
func newOCIConditionalTestClient(t *testing.T) *ocios.ObjectStorageClient {
	t.Helper()
	var (
		provider ocicmn.ConfigurationProvider
		err      error
	)
	switch {
	case cos.IsParseBool(os.Getenv(env.OCIInstancePrincipalAuth)):
		provider, err = ociauth.InstancePrincipalConfigurationProvider()
	case os.Getenv(env.OCIPrivateKey) != "":
		provider = ocicmn.NewRawConfigurationProvider(
			os.Getenv(env.OCITenancyOCID), os.Getenv(env.OCIUserOCID), os.Getenv(env.OCIRegion),
			os.Getenv(env.OCIFingerprint), os.Getenv(env.OCIPrivateKey), nil,
		)
	default:
		path := os.Getenv(env.OCIConfigFilePath)
		if path == "" {
			path = filepath.Join(os.Getenv("HOME"), ".oci", "config")
		}
		profile := os.Getenv(env.OCIProfile)
		if profile == "" {
			profile = "DEFAULT"
		}
		provider, err = ocicmn.ConfigurationProviderFromFileWithProfile(path, profile, "")
	}
	tassert.CheckFatal(t, err)
	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(provider)
	tassert.CheckFatal(t, err)
	if region := os.Getenv(env.OCIRegion); region != "" {
		client.SetRegion(region)
	}
	return &client
}
