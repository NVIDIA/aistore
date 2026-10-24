//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
// no-copyright
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"

	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

type conditionalOCITransport struct {
	sync.Mutex
	value, etag string
	exists      bool
	calls       int
}

func (s *conditionalOCITransport) Do(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	s.Lock()
	defer s.Unlock()
	s.calls++
	status, response := http.StatusOK, "{}"
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	match, none := r.Header.Get("If-Match"), r.Header.Get("If-None-Match")
	if (none == "*" && s.exists) || (match != "" && (!s.exists || match != s.etag)) {
		status, response = http.StatusPreconditionFailed, `{"code":"PreconditionFailed","message":"predicate failed"}`
	} else {
		s.exists, s.value, s.etag = true, string(body), fmt.Sprintf("opaque-%d", s.calls)
		h.Set("ETag", s.etag)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
}
func conditionalOCIBackend(t *testing.T) (*ocibp, *conditionalOCITransport) {
	t.Helper()
	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(newTestOCIConfigProvider(t, "us-ashburn-1"))
	if err != nil {
		t.Fatal(err)
	}
	transport := &conditionalOCITransport{}
	client.HTTPClient = transport
	client.Host = "http://oci.invalid"
	bp := &ocibp{namespace: "ns", defaultRegion: "us-ashburn-1", mpuThreshold: 1 << 30}
	bp.clients.Store("us-ashburn-1", &client)
	return bp, transport
}
func conditionalLOM() *core.LOM {
	lom := &core.LOM{ObjName: "manifest"}
	*lom.Bck() = meta.Bck{Name: "bucket", Provider: apc.OCI, Props: &cmn.Bprops{}}
	lom.SetCksum(cos.NoneCksum)
	return lom
}
func conditionalRequest(header, value string) *http.Request {
	r, _ := http.NewRequest(http.MethodPut, "http://ais.invalid/s3/bucket/manifest", http.NoBody)
	if header != "" {
		r.Header.Set(header, value)
	}
	return r
}
func TestOCIConditionalWire(t *testing.T) {
	bp, transport := conditionalOCIBackend(t)
	put := func(value, header, condition string) (int, error) {
		return bp.PutObj(context.Background(), io.NopCloser(strings.NewReader(value)), conditionalLOM(), conditionalRequest(header, condition))
	}
	if code, err := put("original", "If-None-Match", "*"); code != 0 || err != nil {
		t.Fatalf("initial create: %d %v", code, err)
	}
	original := cmn.QuoteETag(transport.etag)
	if code, err := put("lost", "If-None-Match", "*"); code != 412 || err == nil {
		t.Fatalf("create-only overwrite: %d %v", code, err)
	}
	if transport.value != "original" {
		t.Fatal("rejected create overwrote bytes")
	}
	if code, err := put("lost", "If-Match", `"stale"`); code != 412 || err == nil {
		t.Fatalf("stale ETag: %d %v", code, err)
	}
	if code, err := put("updated", "If-Match", original); code != 0 || err != nil {
		t.Fatalf("valid ETag: %d %v", code, err)
	}
	if transport.value != "updated" {
		t.Fatal("valid replacement was not persisted")
	}
	if code, err := put("lost", "If-Match", original); code != 412 || err == nil {
		t.Fatalf("reused ETag: %d %v", code, err)
	}
}
func TestOCIConditionalConcurrentCreate(t *testing.T) {
	bp, _ := conditionalOCIBackend(t)
	start := make(chan struct{})
	codes := make(chan int, 16)
	for i := range 16 {
		go func() {
			<-start
			code, err := bp.PutObj(context.Background(), io.NopCloser(strings.NewReader(strconv.Itoa(i))), conditionalLOM(), conditionalRequest("If-None-Match", "*"))
			if code == 0 && err != nil {
				code = 500
			}
			codes <- code
		}()
	}
	close(start)
	won, rejected := 0, 0
	for range 16 {
		switch code := <-codes; code {
		case 0:
			won++
		case 412:
			rejected++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if won != 1 || rejected != 15 {
		t.Fatalf("winners=%d rejected=%d", won, rejected)
	}
}
func TestOCIConditionalMultipartCommit(t *testing.T) {
	bp, transport := conditionalOCIBackend(t)
	complete := func(header, value string) (int, error) {
		_, _, code, err := bp.CompleteMpt(context.Background(), conditionalLOM(), conditionalRequest(header, value), "upload", nil, nil)
		return code, err
	}
	if code, err := complete("If-None-Match", "*"); code != 0 || err != nil {
		t.Fatalf("initial completion %d %v", code, err)
	}
	etag, value := transport.etag, transport.value
	if code, err := complete("If-None-Match", "*"); code != 412 || err == nil {
		t.Fatalf("duplicate completion %d %v", code, err)
	}
	if transport.etag != etag || transport.value != value {
		t.Fatal("failed completion changed object")
	}
	if code, err := complete("If-Match", cmn.QuoteETag(etag)); code != 0 || err != nil {
		t.Fatalf("valid completion %d %v", code, err)
	}
}
func TestOCIConditionalInvalidHeader(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"If-None-Match", `"etag"`}, {"If-Match", `W/"weak"`},
		{"If-Match", `"one", "two"`}, {"If-Match", ""}, {"If-Match", `""`},
	} {
		bp, transport := conditionalOCIBackend(t)
		code, err := bp.PutObj(context.Background(), io.NopCloser(strings.NewReader("must-not-write")), conditionalLOM(), conditionalRequest(tc.key, tc.value))
		if code != 400 || err == nil || transport.calls != 0 {
			t.Fatalf("%s=%q code=%d err=%v calls=%d", tc.key, tc.value, code, err, transport.calls)
		}
	}
}

func TestOCIConditionalConcurrentReplace(t *testing.T) {
	bp, transport := conditionalOCIBackend(t)
	put := func(value string, request *http.Request) (int, error) {
		return bp.PutObj(context.Background(), io.NopCloser(strings.NewReader(value)), conditionalLOM(), request)
	}
	if code, err := put("original", conditionalRequest("If-None-Match", "*")); code != 0 || err != nil {
		t.Fatalf("seed: %d %v", code, err)
	}
	etag := cmn.QuoteETag(transport.etag)
	start := make(chan struct{})
	type result struct {
		code  int
		value string
	}
	outcomes := make(chan result, 16)
	for i := range 16 {
		go func() {
			<-start
			value := fmt.Sprintf("writer-%d", i)
			code, err := put(value, conditionalRequest("If-Match", etag))
			if code == 0 && err != nil {
				code = 500
			}
			outcomes <- result{code, value}
		}()
	}
	close(start)
	won, rejected, winner := 0, 0, ""
	for range 16 {
		outcome := <-outcomes
		switch outcome.code {
		case 0:
			won++
			winner = outcome.value
		case 412:
			rejected++
		default:
			t.Fatalf("unexpected status %d", outcome.code)
		}
	}
	if won != 1 || rejected != 15 || transport.value != winner {
		t.Fatalf("winners=%d rejected=%d stored=%q winner=%q", won, rejected, transport.value, winner)
	}
}
