//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// quote a single If-Match ETag and preserve other forms
func TestAWSWriteConditions(t *testing.T) {
	for _, tc := range []struct{ key, value, match, none string }{
		{cos.HdrIfMatch, `"etag-1"`, `"etag-1"`, ""},
		{cos.HdrIfMatch, "etag-1", `"etag-1"`, ""},
		{cos.HdrIfMatch, "*", "*", ""},
		{cos.HdrIfMatch, `"*"`, `"*"`, ""},
		{cos.HdrIfMatch, `"one", "two"`, `"one", "two"`, ""},
		{cos.HdrIfMatch, `"one,two"`, `"one,two"`, ""},
		{cos.HdrIfNoneMatch, "*", "", "*"},
	} {
		r := &http.Request{Header: http.Header{tc.key: {tc.value}}}
		match, none, ecode, err := awsWriteConditions(r)
		if err != nil || ecode != 0 || aws.ToString(match) != tc.match || aws.ToString(none) != tc.none {
			t.Fatalf("%s=%q: match=%q none=%q ecode=%d err=%v", tc.key, tc.value, aws.ToString(match), aws.ToString(none), ecode, err)
		}
	}
	r := &http.Request{Header: http.Header{cos.HdrIfMatch: {`""`}}}
	if _, _, ecode, err := awsWriteConditions(r); err == nil || ecode != http.StatusBadRequest {
		t.Fatalf("empty If-Match: expected 400, got %d %v", ecode, err)
	}
}

// fake S3: CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload
type condS3Transport struct {
	match, none string
	status      int    // CompleteMultipartUpload
	code        string // ditto
	parts       int
	aborts      int
	completes   int
	mu          sync.Mutex
}

func (s *condS3Transport) Do(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		q      = r.URL.Query()
		status = http.StatusOK
		body   string
		h      = make(http.Header)
	)
	_, uploads := q["uploads"]
	switch {
	case r.Method == http.MethodPost && uploads:
		body = `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>obj</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`
	case r.Method == http.MethodPut && q.Get("partNumber") != "":
		s.parts++
		h.Set(cos.HdrETag, `"part-etag"`)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.aborts++
		status = http.StatusNoContent
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		s.completes++
		s.match, s.none = r.Header.Get(cos.HdrIfMatch), r.Header.Get(cos.HdrIfNoneMatch)
		status = s.status
		if status != http.StatusOK {
			body = `<Error><Code>` + s.code + `</Code><Message>rejected</Message></Error>`
		} else {
			body = `<CompleteMultipartUploadResult><ETag>"final-etag"</ETag></CompleteMultipartUploadResult>`
		}
	default:
		status, body = http.StatusNotImplemented, `<Error><Code>NotImplemented</Code><Message>unexpected request</Message></Error>`
	}
	h.Set(cos.HdrContentType, "application/xml")
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func condS3Upload(t *testing.T, transport *condS3Transport, size int, match, none *string) error {
	t.Helper()
	svc := s3.New(s3.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String("http://s3.invalid"),
		UsePathStyle:     true,
		HTTPClient:       transport,
		Credentials:      aws.AnonymousCredentials{},
		RetryMaxAttempts: 1,
	})
	uploader := s3manager.NewUploader(svc, func(u *s3manager.Uploader) {
		u.PartSize = s3manager.MinUploadPartSize
		u.Concurrency = 1
	})
	_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String("bucket"),
		Key:         aws.String("obj"),
		Body:        bytes.NewReader(make([]byte, size)),
		IfMatch:     match,
		IfNoneMatch: none,
	})
	return err
}

// guard the uploader (s3manager) behavior that conditional writes depend on:
// - copy write conditions from PutObjectInput to CompleteMultipartUploadInput
// - abort multipart upload upon rejection
func TestAWSUploaderConditions(t *testing.T) {
	const multipart = int(2*s3manager.MinUploadPartSize + 1)
	bck := &cmn.Bck{Name: "bucket", Provider: apc.AWS}
	for _, tc := range []struct {
		name        string
		status      int
		code        string
		match, none *string
	}{
		{"success", http.StatusOK, "", aws.String(`"etag-1"`), nil},
		{"precondition", http.StatusPreconditionFailed, "PreconditionFailed", nil, aws.String("*")},
		{"conflict", http.StatusConflict, cmn.TcodeCondWriteConflict, aws.String(`"etag-1"`), nil},
		{"other-conflict", http.StatusConflict, "OperationAborted", nil, aws.String("*")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &condS3Transport{status: tc.status, code: tc.code}
			err := condS3Upload(t, transport, multipart, tc.match, tc.none)

			if transport.match != aws.ToString(tc.match) || transport.none != aws.ToString(tc.none) {
				t.Fatalf("conditions on the wire: match=%q none=%q", transport.match, transport.none)
			}
			if transport.parts < 2 || transport.completes != 1 {
				t.Fatalf("expecting multipart: parts=%d completes=%d", transport.parts, transport.completes)
			}
			wantAborts := 0
			if tc.status != http.StatusOK {
				wantAborts = 1
			}
			if transport.aborts != wantAborts {
				t.Fatalf("aborts=%d, want %d", transport.aborts, wantAborts)
			}
			if tc.status == http.StatusOK {
				if err != nil {
					t.Fatal(err)
				}
				return
			}

			ecode, errV := awsWriteError(err, bck, "obj", true)
			if ecode != tc.status {
				t.Fatalf("status %d, want %d (%v)", ecode, tc.status, errV)
			}
			switch {
			case tc.status == http.StatusPreconditionFailed:
				if !cos.IsErrPreconditionFailed(errV) {
					t.Fatalf("expecting precondition-failed error, got %v", errV)
				}
			case tc.code == cmn.TcodeCondWriteConflict:
				if herr := cmn.AsErrHTTP(errV); herr == nil || herr.TypeCode != cmn.TcodeCondWriteConflict {
					t.Fatalf("lost conditional-conflict type code: %v", errV)
				}
			default:
				if herr := cmn.AsErrHTTP(errV); herr != nil && herr.TypeCode == cmn.TcodeCondWriteConflict {
					t.Fatalf("unrelated conflict misclassified: %v", errV)
				}
			}
		})
	}
}

// respond to every request with the same status; count requests
type retryS3Transport struct {
	status int
	code   string
	n      atomic.Int32
}

func (s *retryS3Transport) Do(r *http.Request) (*http.Response, error) {
	s.n.Add(1)
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	h := make(http.Header)
	h.Set(cos.HdrContentType, "application/xml")
	body := `<Error><Code>` + s.code + `</Code><Message>retry test</Message></Error>`
	if r.Method == http.MethodHead {
		body = ""
	}
	return &http.Response{StatusCode: s.status, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func newRetryS3Client(transport *retryS3Transport) *s3.Client {
	sr := sdkRetryDflt // production settings, no backoff
	sr.noBackoff = true
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://s3.invalid"),
		UsePathStyle: true,
		HTTPClient:   transport,
		Credentials:  aws.AnonymousCredentials{},
		Retryer:      sr.s3(),
	})
}

// client retryer: at most cmn.BackendMaxAttempts; with cmn.WithNoRetry: exactly one
func TestAWSRetryAttempts(t *testing.T) {
	const (
		bucket = "bucket"
		key    = "obj"
	)
	ops := []struct {
		name string
		call func(ctx context.Context, svc *s3.Client) error
	}{
		{"head", func(ctx context.Context, svc *s3.Client) error {
			_, err := svc.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}, awsCallOpts(ctx)...)
			return err
		}},
		{"get", func(ctx context.Context, svc *s3.Client) error {
			_, err := svc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}, awsCallOpts(ctx)...)
			return err
		}},
		{"list", func(ctx context.Context, svc *s3.Client) error {
			_, err := svc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, awsCallOpts(ctx)...)
			return err
		}},
		{"delete", func(ctx context.Context, svc *s3.Client) error {
			_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}, awsCallOpts(ctx)...)
			return err
		}},
		{"put", func(ctx context.Context, svc *s3.Client) error {
			uploader := s3manager.NewUploader(svc)
			uploader.ClientOptions = append(uploader.ClientOptions, awsCallOpts(ctx)...)
			_, err := uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(make([]byte, 1024)),
			})
			return err
		}},
	}
	statuses := []struct {
		status int
		code   string
	}{
		{http.StatusServiceUnavailable, "SlowDown"},
		{http.StatusTooManyRequests, "SlowDown"},
		{http.StatusInternalServerError, "InternalError"},
	}
	for _, op := range ops {
		for _, st := range statuses {
			for _, noRetry := range []bool{false, true} {
				ctx := context.Background()
				want := int32(cmn.BackendMaxAttempts)
				if noRetry {
					ctx = cmn.WithNoRetry(ctx)
					want = 1
				}
				// HEAD response has no body: bare 429 (no error code) is not retryable for the SDK
				if op.name == "head" && st.status == http.StatusTooManyRequests {
					want = 1
				}
				transport := &retryS3Transport{status: st.status, code: st.code}
				svc := newRetryS3Client(transport)

				err := op.call(ctx, svc)
				tassert.Fatalf(t, err != nil, "%s(%d, no-retry=%t): expected error", op.name, st.status, noRetry)
				n := transport.n.Load()
				tassert.Fatalf(t, n == want, "%s(%d, no-retry=%t): expected %d request(s), got %d",
					op.name, st.status, noRetry, want, n)
			}
		}
	}
}

// AWS_MAX_ATTEMPTS and profile's max_attempts: take precedence over the client default
// but not over cmn.WithNoRetry
func TestAWSRetryMaxAttemptsPrecedence(t *testing.T) {
	orig := sdkRetryDflt
	sdkRetryDflt.noBackoff = true
	t.Cleanup(func() { sdkRetryDflt = orig })

	tests := []struct {
		name         string
		env, profile string
		wantUnmarked int32 // zero for invalid settings
	}{
		{"default", "", "", cmn.BackendMaxAttempts},
		{"env", "4", "", 4},
		{"profile", "", "3", 3},
		{"env-over-profile", "4", "3", 4},
		{"negative-env", "-1", "3", 0},
		{"negative-profile", "", "-1", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			conf := "[default]\nregion = us-east-1\n"
			if tc.profile != "" {
				conf += "max_attempts = " + tc.profile + "\n"
			}
			confFile := filepath.Join(dir, "config")
			tassert.CheckFatal(t, os.WriteFile(confFile, []byte(conf), 0o600))
			// isolate from ambient AWS configuration
			t.Setenv("AWS_CONFIG_FILE", confFile)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
			t.Setenv("AWS_PROFILE", "")
			t.Setenv("AWS_CA_BUNDLE", "")
			t.Setenv("AWS_ACCESS_KEY_ID", "fake")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "fake")
			t.Setenv("AWS_MAX_ATTEMPTS", tc.env)

			if tc.wantUnmarked == 0 {
				_, err := (&sessConf{}).s3client("")
				tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "invalid AWS retry attempt limit"),
					"expected invalid attempt limit error, got %v", err)
				return
			}
			cfg, err := (&sessConf{}).awsLoadConfig()
			tassert.CheckFatal(t, err)
			for _, noRetry := range []bool{false, true} {
				transport := &retryS3Transport{status: http.StatusServiceUnavailable, code: "SlowDown"}
				cfg.HTTPClient = transport
				svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
					o.BaseEndpoint = aws.String("http://s3.invalid")
					o.UsePathStyle = true
				})
				ctx, want := context.Background(), tc.wantUnmarked
				if noRetry {
					ctx, want = cmn.WithNoRetry(ctx), 1
				}
				_, err = svc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("obj")}, awsCallOpts(ctx)...)
				tassert.Fatalf(t, err != nil, "no-retry=%t: expected error", noRetry)
				n := transport.n.Load()
				tassert.Fatalf(t, n == want, "no-retry=%t: expected %d request(s), got %d", noRetry, want, n)
			}
		})
	}
}
