//go:build aws

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAWSWriteConditionsSDKEndpoint(t *testing.T) {
	const endpoint = "https://s3.example.invalid"
	// isolate from ambient AWS configuration (CA bundle, profile, shared files)
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CA_BUNDLE", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", endpoint)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_ACCESS_KEY_ID", "fake")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fake")
	sc := sessConf{}
	cfg, err := sc.awsLoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	svc := s3.NewFromConfig(cfg)
	if got := aws.ToString(svc.Options().BaseEndpoint); got != endpoint || awsSupportsCondWrite(svc) {
		t.Fatalf("SDK endpoint %q must disable conditional writes", got)
	}
	r := &http.Request{Header: http.Header{cos.HdrIfNoneMatch: {"*"}}}
	match, none, ecode, err := awsWriteConditions(svc, r)
	var unsupported *cmn.ErrUnsupp
	if match != nil || none != nil || ecode != http.StatusNotImplemented || !errors.As(err, &unsupported) {
		t.Fatalf("expected unsupported conditional write, got status %d and error %v", ecode, err)
	}
}

// default endpoint: forward conditions, quoted If-Match
func TestAWSWriteConditionsQuoting(t *testing.T) {
	svc := s3.New(s3.Options{Region: "us-east-1"})
	for _, tc := range []struct{ key, value, match, none string }{
		{cos.HdrIfMatch, `"etag-1"`, `"etag-1"`, ""},
		{cos.HdrIfMatch, "etag-1", `"etag-1"`, ""},
		{cos.HdrIfNoneMatch, "*", "", "*"},
	} {
		r := &http.Request{Header: http.Header{tc.key: {tc.value}}}
		match, none, ecode, err := awsWriteConditions(svc, r)
		if err != nil || ecode != 0 || aws.ToString(match) != tc.match || aws.ToString(none) != tc.none {
			t.Fatalf("%s=%q: match=%q none=%q ecode=%d err=%v", tc.key, tc.value, aws.ToString(match), aws.ToString(none), ecode, err)
		}
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
