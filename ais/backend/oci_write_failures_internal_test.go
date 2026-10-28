//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
// no-copyright
package backend

import (
	"container/list"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"

	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
)

type failingOCIWriteTransport struct {
	match, none   string
	errorCode     string
	abortUploadID string
	status        int
	abortStatus   int
	aborts        int
	failPart      bool
	abortBounded  bool
}

func (s *failingOCIWriteTransport) Do(r *http.Request) (*http.Response, error) {
	status, body := s.status, `{"code":"PreconditionFailed","message":"rejected condition"}`
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	if s.status == http.StatusConflict {
		body = `{"code":"ConcurrentObjectUpdate","message":"competing update"}`
	}
	if s.errorCode != "" {
		body = `{"code":"` + s.errorCode + `","message":"rejected condition"}`
	}
	switch {
	case r.Method == http.MethodDelete:
		s.aborts++
		s.abortUploadID = r.URL.Query().Get("uploadId")
		deadline, ok := r.Context().Deadline()
		s.abortBounded = ok && r.Context().Err() == nil && time.Until(deadline) > 0 && time.Until(deadline) <= time.Minute
		status, body = http.StatusNoContent, ""
		if s.abortStatus != 0 {
			status, body = s.abortStatus, `{"code":"AbortRejected","message":"cleanup failed"}`
		}
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/u"):
		status, body = http.StatusOK, `{"uploadId":"upload","object":"manifest"}`
	case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/u/") && !s.failPart:
		status, body = http.StatusOK, ""
		h.Set("ETag", "part-etag")
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/u/"):
		s.match, s.none = r.Header.Get("If-Match"), r.Header.Get("If-None-Match")
		if status == http.StatusOK {
			h.Set("ETag", "object-etag")
		}
	case s.status == http.StatusOK:
		h.Set("ETag", "object-etag")
	}
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func TestOCIConditionalConflictCode(t *testing.T) {
	for _, conditional := range []bool{false, true} {
		t.Run(strconv.FormatBool(conditional), func(t *testing.T) {
			bp, _ := conditionalOCIBackend(t)
			client, _, err := bp.ociClient(conditionalLOM().Bck().RemoteBck())
			if err != nil {
				t.Fatal(err)
			}
			client.HTTPClient = &failingOCIWriteTransport{status: http.StatusConflict}
			request := conditionalRequest("", "")
			if conditional {
				request = conditionalRequest("If-None-Match", "*")
			}
			status, err := bp.PutObj(t.Context(), io.NopCloser(strings.NewReader("value")), conditionalLOM(), request)
			if status != http.StatusConflict || err == nil {
				t.Fatalf("status=%d err=%v", status, err)
			}
			wire := cmn.AsErrHTTP(err)
			if conditional && (wire == nil || wire.TypeCode != "ConditionalRequestConflict") {
				t.Fatalf("incorrect conditional error: %v", err)
			}
			if !conditional && wire != nil && wire.TypeCode == "ConditionalRequestConflict" {
				t.Fatal("unconditional conflict was misclassified")
			}
		})
	}
}

func TestOCIConditionalAutomaticMultipart(t *testing.T) {
	tests := []struct {
		name, errorCode string
		status          int
		abortStatus     int
		wantAborts      int
	}{
		{name: "success", status: http.StatusOK},
		{name: "precondition", status: http.StatusPreconditionFailed, wantAborts: 1},
		{name: "conflict", status: http.StatusConflict, wantAborts: 1},
		{name: "precondition-abort-fails", status: http.StatusPreconditionFailed, abortStatus: http.StatusBadRequest, wantAborts: 1},
		{name: "conflict-abort-fails", status: http.StatusConflict, abortStatus: http.StatusBadRequest, wantAborts: 1},
		{name: "other-conflict", status: http.StatusConflict, errorCode: "UnrelatedConflict"},
		{name: "server-error", status: http.StatusInternalServerError, errorCode: "InternalServerError"},
	}
	for _, test := range tests {
		for _, header := range []string{"If-Match", "If-None-Match"} {
			t.Run(test.name+"/"+header, func(t *testing.T) {
				bp, _ := conditionalOCIBackend(t)
				bp.mpuThreshold, bp.mpuSegmentMaxSize, bp.mpuMaxThreads, bp.mpThreadPoolSize = 4, 16, 1, 1
				bp.mpChildPendingList = list.New()
				lom := conditionalLOM()
				lom.SetSize(8)
				client, _, err := bp.ociClient(lom.Bck().RemoteBck())
				if err != nil {
					t.Fatal(err)
				}
				retry := ocicmn.NoRetryPolicy()
				client.Configuration.RetryPolicy = &retry
				transport := &failingOCIWriteTransport{status: test.status, errorCode: test.errorCode, abortStatus: test.abortStatus}
				client.HTTPClient = transport
				value := "*"
				if header == "If-Match" {
					value = `"opaque-etag"`
				}
				code, err := bp.PutObj(t.Context(), io.NopCloser(strings.NewReader("original")), lom, conditionalRequest(header, value))
				wantCode := test.status
				if test.status == http.StatusOK {
					wantCode = 0
				}
				if code != wantCode || (err == nil) != (test.status == http.StatusOK) {
					t.Fatalf("code=%d err=%v", code, err)
				}
				if err != nil && strings.Contains(err.Error(), "AbortRejected") {
					t.Fatalf("cleanup replaced the original commit error: %v", err)
				}
				if test.status == http.StatusConflict && test.errorCode == "" {
					wire := cmn.AsErrHTTP(err)
					if wire == nil || wire.TypeCode != "ConditionalRequestConflict" {
						t.Fatalf("lost conditional conflict identity: %v", err)
					}
				}
				if (header == "If-Match" && (transport.match != "opaque-etag" || transport.none != "")) ||
					(header == "If-None-Match" && (transport.none != "*" || transport.match != "")) {
					t.Fatalf("commit conditions: match=%q none=%q", transport.match, transport.none)
				}
				if transport.aborts != test.wantAborts {
					t.Fatalf("aborts=%d, want %d", transport.aborts, test.wantAborts)
				}
				if test.wantAborts != 0 && (!transport.abortBounded || transport.abortUploadID != "upload") {
					t.Fatalf("abort must have a bounded live context and the rejected upload ID: bounded=%t id=%q", transport.abortBounded, transport.abortUploadID)
				}
			})
		}
	}
}

// preserve the existing abort when uploading a part fails.
func TestOCIAutomaticMultipartPartFailure(t *testing.T) {
	bp, _ := conditionalOCIBackend(t)
	bp.mpuThreshold, bp.mpuSegmentMaxSize, bp.mpuMaxThreads, bp.mpThreadPoolSize = 4, 16, 1, 1
	bp.mpChildPendingList = list.New()
	lom := conditionalLOM()
	lom.SetSize(8)
	client, _, err := bp.ociClient(lom.Bck().RemoteBck())
	if err != nil {
		t.Fatal(err)
	}
	transport := &failingOCIWriteTransport{status: http.StatusBadRequest, failPart: true}
	client.HTTPClient = transport
	code, err := bp.PutObj(t.Context(), io.NopCloser(strings.NewReader("original")), lom, conditionalRequest("If-None-Match", "*"))
	if err == nil || transport.aborts != 1 {
		t.Fatalf("code=%d err=%v aborts=%d, want failed upload and one abort", code, err, transport.aborts)
	}
	if transport.match != "" || transport.none != "" {
		t.Fatal("failed part upload reached multipart completion")
	}
}
