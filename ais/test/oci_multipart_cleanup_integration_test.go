//go:build oci

// Package integration_test.
// no-copyright
package integration_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
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

// opt in when the test client itself can inspect and abort native OCI uploads.
func TestS3OCIAutomaticMultipartCleanup(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, RequiredCloudProvider: apc.OCI})
	if os.Getenv("AIS_TEST_OCI_NATIVE_AUTH") != "true" {
		t.Skip("set AIS_TEST_OCI_NATIVE_AUTH=true with native OCI test-client credentials")
	}
	const size = 128 * cos.KiB
	threshold, err := cos.ParseSize(os.Getenv(env.OCIMultiPartUploadThreshold), "")
	if err != nil || threshold <= 0 || threshold >= size {
		t.Fatal("set OCI_MULTI_PART_UPLOAD_THRESHOLD below 128KiB in both daemons and tests")
	}
	provider := ocicmn.DefaultConfigProvider()
	if cos.IsParseBool(os.Getenv(env.OCIInstancePrincipalAuth)) {
		provider, err = ociauth.InstancePrincipalConfigurationProvider()
		tassert.CheckFatal(t, err)
	}
	native, err := ocios.NewObjectStorageClientWithConfigurationProvider(provider)
	tassert.CheckFatal(t, err)
	if region := os.Getenv(env.OCIRegion); region != "" {
		native.SetRegion(region)
	}
	namespace, err := native.GetNamespace(t.Context(), ocios.GetNamespaceRequest{})
	tassert.CheckFatal(t, err)
	prefix := "conditional-cleanup-" + trand.String(12) + "/"
	object := prefix + "object"
	t.Logf("isolated object prefix: %s", prefix)
	list := func(ctx context.Context) []ocios.MultipartUpload {
		var uploads []ocios.MultipartUpload
		req := ocios.ListMultipartUploadsRequest{NamespaceName: namespace.Value, BucketName: &cliBck.Name}
		for {
			response, err := native.ListMultipartUploads(ctx, req)
			tassert.CheckFatal(t, err)
			for _, upload := range response.Items {
				if upload.Object != nil && strings.HasPrefix(*upload.Object, prefix) {
					uploads = append(uploads, upload)
				}
			}
			if response.OpcNextPage == nil || *response.OpcNextPage == "" {
				break
			}
			req.Page = response.OpcNextPage
		}
		return uploads
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		for _, upload := range list(ctx) {
			_, err := native.AbortMultipartUpload(ctx, ocios.AbortMultipartUploadRequest{NamespaceName: namespace.Value, BucketName: &cliBck.Name, ObjectName: upload.Object, UploadId: upload.UploadId})
			tassert.CheckError(t, err)
		}
		tassert.CheckError(t, api.DeleteObject(baseParams, cliBck, object))
	})
	_, err = api.HeadBucket(baseParams, cliBck, false)
	tassert.CheckFatal(t, err)
	tools.EnableClusterFeatures(t, feat.S3ReverseProxy)
	client := s3.New(s3.Options{HTTPClient: newS3Client(true), Region: env.AwsDefaultRegion(), BaseEndpoint: aws.String(proxyURL), UsePathStyle: true, Credentials: getS3Credentials(t)}, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(&addGetBodyMiddleware{}, middleware.After)
		})
	})
	data := bytes.Repeat([]byte("a"), size)
	put := func(match, none *string) error {
		_, err := client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: &cliBck.Name, Key: &object, Body: bytes.NewReader(data), IfMatch: match, IfNoneMatch: none})
		return err
	}
	tassert.CheckFatal(t, put(nil, aws.String("*")))
	checkOCIWritePrecondition(t, put(nil, aws.String("*")))
	checkOCIWritePrecondition(t, put(aws.String(`"stale"`), nil))
	tassert.Errorf(t, len(list(t.Context())) == 0, "failed automatic multipart commit leaked a native upload")
}
