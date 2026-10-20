//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
// no-copyright
package backend

import (
	"container/list"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

type failingOCIWriteTransport struct {
	status   int
	aborts   atomic.Int32
	creates  atomic.Int32
	failPart bool
}

func (s *failingOCIWriteTransport) Do(r *http.Request) (*http.Response, error) {
	status, body := s.status, `{"code":"PreconditionFailed","message":"rejected condition"}`
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	if s.status == http.StatusConflict {
		body = `{"code":"ConcurrentObjectUpdate","message":"competing update"}`
	}
	switch {
	case r.Method == http.MethodDelete:
		s.aborts.Add(1)
		status, body = http.StatusNoContent, ""
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/u"):
		s.creates.Add(1)
		status, body = http.StatusOK, `{"uploadId":"upload","object":"manifest"}`
	case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/u/") && !s.failPart:
		status, body = http.StatusOK, ""
		h.Set("ETag", "part-etag")
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

func TestOCIAutomaticMultipartAbort(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusPreconditionFailed, http.StatusConflict} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			bp, _ := conditionalOCIBackend(t)
			bp.mpuThreshold, bp.mpuSegmentMaxSize, bp.mpuMaxThreads, bp.mpThreadPoolSize = 4, 16, 1, 1
			bp.mpChildPendingList = list.New()
			lom := conditionalLOM()
			lom.SetSize(8)
			client, _, err := bp.ociClient(lom.Bck().RemoteBck())
			if err != nil {
				t.Fatal(err)
			}
			transport := &failingOCIWriteTransport{status: status}
			client.HTTPClient = transport
			code, err := bp.PutObj(context.Background(), io.NopCloser(strings.NewReader("original")), lom, conditionalRequest("If-None-Match", "*"))
			wantCode := status
			wantAborts := int32(1)
			if status == http.StatusOK {
				wantCode, wantAborts = 0, 0
			}
			if code != wantCode || (err == nil) != (status == http.StatusOK) {
				t.Fatalf("code=%d err=%v", code, err)
			}
			if transport.creates.Load() != 1 || transport.aborts.Load() != wantAborts {
				t.Fatalf("creates=%d aborts=%d, want %d", transport.creates.Load(), transport.aborts.Load(), wantAborts)
			}
			if status == http.StatusConflict {
				wire := cmn.AsErrHTTP(err)
				if wire == nil || wire.TypeCode != "ConditionalRequestConflict" {
					t.Fatalf("wrong error: %v", err)
				}
			}
		})
	}
}
