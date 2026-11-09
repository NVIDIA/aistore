//go:build oci

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"

	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

type testOCIConfigProvider struct {
	key    *rsa.PrivateKey
	region string
}

func newTestOCIConfigProvider(t *testing.T, region string) ocicmn.ConfigurationProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate test RSA key: %v", err)
	}
	return testOCIConfigProvider{key: key, region: region}
}

func (p testOCIConfigProvider) Region() (string, error)       { return p.region, nil }
func (testOCIConfigProvider) KeyFingerprint() (string, error) { return "aa:bb:cc", nil }
func (testOCIConfigProvider) UserOCID() (string, error) {
	return "ocid1.user.oc1..exampleuniqueID", nil
}
func (testOCIConfigProvider) TenancyOCID() (string, error) {
	return "ocid1.tenancy.oc1..exampleuniqueID", nil
}
func (p testOCIConfigProvider) PrivateRSAKey() (*rsa.PrivateKey, error) { return p.key, nil }
func (testOCIConfigProvider) KeyID() (string, error) {
	return "ocid1.tenancy.oc1..example/ocid1.user.oc1..example/aa:bb:cc", nil
}
func (testOCIConfigProvider) AuthType() (ocicmn.AuthConfig, error) {
	return ocicmn.AuthConfig{AuthType: ocicmn.UserPrincipal}, nil
}

func TestOCIRegionForBck(t *testing.T) {
	bp := &ocibp{defaultRegion: "us-ashburn-1"}

	if got := bp.regionForBck(nil); got != "us-ashburn-1" {
		t.Fatalf("expected default region, got %q", got)
	}

	bck := &cmn.Bck{
		Name:     "bucket",
		Provider: apc.OCI,
		Props: &cmn.Bprops{
			Extra: cmn.ExtraProps{
				OCI: cmn.ExtraPropsOCI{Region: "us-phoenix-1"},
			},
		},
	}
	if got := bp.regionForBck(bck); got != "us-phoenix-1" {
		t.Fatalf("expected per-bucket region, got %q", got)
	}
}

func TestOCIFetchCliConfigInstancePrincipalExplicitRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	var gotRegion string
	newOCIInstancePrincipalProvider = func(region string) (ocicmn.ConfigurationProvider, error) {
		gotRegion = region
		return newTestOCIConfigProvider(t, "us-phoenix-1"), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")
	t.Setenv(env.OCIRegion, "us-phoenix-1")
	t.Setenv(env.OCICompartmentOCID, "ocid1.compartment.oc1..example")

	bp := &ocibp{}
	if err := bp.fetchCliConfig(); err != nil {
		t.Fatalf("fetchCliConfig failed: %v", err)
	}
	if gotRegion != "us-phoenix-1" {
		t.Fatalf("expected explicit region to be passed through, got %q", gotRegion)
	}
	if bp.defaultRegion != "us-phoenix-1" {
		t.Fatalf("expected default region to be set, got %q", bp.defaultRegion)
	}
	if bp.compartmentOCID != "ocid1.compartment.oc1..example" {
		t.Fatalf("expected compartment OCID to be preserved, got %q", bp.compartmentOCID)
	}
}

func TestOCIFetchCliConfigInstancePrincipalInferredRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	newOCIInstancePrincipalProvider = func(region string) (ocicmn.ConfigurationProvider, error) {
		if region != "" {
			t.Fatalf("expected empty region override, got %q", region)
		}
		return newTestOCIConfigProvider(t, "us-ashburn-1"), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")
	t.Setenv(env.OCIRegion, "")

	bp := &ocibp{}
	if err := bp.fetchCliConfig(); err != nil {
		t.Fatalf("fetchCliConfig failed: %v", err)
	}
	if bp.defaultRegion != "us-ashburn-1" {
		t.Fatalf("expected provider region to be used, got %q", bp.defaultRegion)
	}
}

func TestOCIFetchCliConfigInstancePrincipalRequiresRegion(t *testing.T) {
	orig := newOCIInstancePrincipalProvider
	defer func() { newOCIInstancePrincipalProvider = orig }()

	newOCIInstancePrincipalProvider = func(_ string) (ocicmn.ConfigurationProvider, error) {
		return newTestOCIConfigProvider(t, ""), nil
	}

	t.Setenv(env.OCIInstancePrincipalAuth, "true")
	t.Setenv(env.OCIRegion, "")

	bp := &ocibp{}
	err := bp.fetchCliConfig()
	if err == nil {
		t.Fatal("expected missing region to fail")
	}
	if !strings.Contains(err.Error(), env.OCIRegion) {
		t.Fatalf("expected error to mention %s, got %v", env.OCIRegion, err)
	}
}

func TestOCIClientCreatesRegionClientOnce(t *testing.T) {
	bp := &ocibp{
		defaultRegion:         "us-ashburn-1",
		configurationProvider: newTestOCIConfigProvider(t, "us-ashburn-1"),
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	clients := make(chan *ocios.ObjectStorageClient, 8)
	errs := make(chan error, 8)

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, _, err := bp.ociClient(nil)
			if err == nil {
				clients <- client
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(clients)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ociClient failed: %v", err)
		}
	}
	var first *ocios.ObjectStorageClient
	for client := range clients {
		if first == nil {
			first = client
			continue
		}
		if first != client {
			t.Fatal("expected all goroutines to receive the same cached client instance")
		}
	}
}

// preserve the SDK error when classifying 429 and 503 for AIS retries
func TestOCIErrorToAISErrorThrottling(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			errSDK := errors.New(http.StatusText(status))
			resp := ocios.GetObjectResponse{RawResponse: &http.Response{StatusCode: status}}
			code, err := ociErrorToAISError("GetObj", "test-bucket", "test/object", "", errSDK, resp)
			if code != status || err == nil || !cmn.IsErrTooManyRequests(err) {
				t.Fatalf("code=%d err=%v, want throttling error %d", code, err, status)
			}
			if msg := err.Error(); msg != errSDK.Error() {
				t.Fatalf("error text should carry the SDK message, got %q", msg)
			}
		})
	}
}

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
	if code, err := put("bare", "If-Match", transport.etag); code != 0 || err != nil {
		t.Fatalf("bare ETag: %d %v", code, err)
	}
	if transport.value != "bare" {
		t.Fatal("bare-ETag replacement was not persisted")
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
		{"If-Match", "*"}, {"If-Match", "one,two"},
	} {
		bp, transport := conditionalOCIBackend(t)
		code, err := bp.PutObj(context.Background(), io.NopCloser(strings.NewReader("must-not-write")), conditionalLOM(), conditionalRequest(tc.key, tc.value))
		if code != 400 || err == nil || transport.calls != 0 {
			t.Fatalf("%s=%q code=%d err=%v calls=%d", tc.key, tc.value, code, err, transport.calls)
		}
	}
}

type failingOCIWriteTransport struct {
	match, none   string
	errorCode     string
	abortUploadID string
	status        int
	abortStatus   int
	aborts        int
	failPart      bool
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
			if conditional && (wire == nil || wire.TypeCode != cmn.TcodeCondWriteConflict) {
				t.Fatalf("incorrect conditional error: %v", err)
			}
			if !conditional && wire != nil && wire.TypeCode == cmn.TcodeCondWriteConflict {
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
		{name: "precondition-abort-fails", status: http.StatusPreconditionFailed, abortStatus: http.StatusServiceUnavailable, wantAborts: 1},
		{name: "conflict-abort-fails", status: http.StatusConflict, abortStatus: http.StatusBadRequest, wantAborts: 1},
		{name: "other-conflict", status: http.StatusConflict, errorCode: "UnrelatedConflict", wantAborts: 1},
		{name: "server-error", status: http.StatusInternalServerError, errorCode: "InternalServerError", wantAborts: 1},
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
				retry := ocicmn.NewRetryPolicy(2, ocicmn.DefaultShouldRetryOperation, func(ocicmn.OCIOperationResponse) time.Duration { return 0 })
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
					if wire == nil || wire.TypeCode != cmn.TcodeCondWriteConflict {
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
				if test.wantAborts != 0 && transport.abortUploadID != "upload" {
					t.Fatalf("abort must use the failed upload ID: id=%q", transport.abortUploadID)
				}
			})
		}
	}
}

// abort the upload and preserve throttling errors from failed parts
func TestOCIAutomaticMultipartPartFailure(t *testing.T) {
	for _, tc := range []struct {
		status, wantCode int
		errorCode        string
	}{
		{http.StatusBadRequest, 0, "InvalidParameter"},
		{http.StatusTooManyRequests, http.StatusTooManyRequests, "TooManyRequests"},
		{http.StatusServiceUnavailable, http.StatusServiceUnavailable, "ServiceUnavailable"},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			bp, _ := conditionalOCIBackend(t)
			bp.mpuThreshold, bp.mpuSegmentMaxSize, bp.mpuMaxThreads, bp.mpThreadPoolSize = 4, 16, 1, 1
			bp.mpChildPendingList = list.New()
			lom := conditionalLOM()
			lom.SetSize(8)
			client, _, err := bp.ociClient(lom.Bck().RemoteBck())
			tassert.CheckFatal(t, err)
			transport := &failingOCIWriteTransport{status: tc.status, errorCode: tc.errorCode, failPart: true}
			client.HTTPClient = transport
			ctx := cmn.WithNoRetry(t.Context())
			code, err := bp.PutObj(ctx, io.NopCloser(strings.NewReader("original")), lom, conditionalRequest("If-None-Match", "*"))
			if err == nil || transport.aborts != 1 {
				t.Fatalf("code=%d err=%v aborts=%d, want failed upload and one abort", code, err, transport.aborts)
			}
			if code != tc.wantCode || cmn.IsErrTooManyRequests(err) != (tc.wantCode != 0) {
				t.Fatalf("code=%d err=%v, want code %d", code, err, tc.wantCode)
			}
			if transport.match != "" || transport.none != "" {
				t.Fatal("failed part upload reached multipart completion")
			}
		})
	}
}

const ociMetadataRangeETag = "remote-etag"

func TestOCIRangeReadsPreserveObjectAttributes(t *testing.T) {
	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(newTestOCIConfigProvider(t, "us-ashburn-1"))
	tassert.CheckFatal(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-Match"); got != ociMetadataRangeETag {
			t.Errorf("If-Match = %q, want %q", got, ociMetadataRangeETag)
		}
		serveOCIMetadataRange(w, r)
	}))
	t.Cleanup(server.Close)
	client.Host = server.URL
	bp := &ocibp{namespace: "namespace", defaultRegion: "us-ashburn-1"}
	bp.clients.Store(bp.defaultRegion, &client)
	lom := &core.LOM{ObjName: "object"}
	lom.Bck().Name, lom.Bck().Provider = "bucket", apc.OCI
	lom.ObjAttrs().Size = 20
	lom.ObjAttrs().CustomMD = cos.StrKVs{"sentinel": "unchanged"}
	before := *lom.ObjAttrs()
	before.CustomMD = maps.Clone(before.CustomMD)
	// Check once before concurrency so a metadata mutation fails deterministically.
	checkOCIMetadataRange(t, bp, lom)
	tassert.Fatalf(t, reflect.DeepEqual(*lom.ObjAttrs(), before), "range GET changed object attributes")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { checkOCIMetadataRange(t, bp, lom) })
	}
	wg.Wait()
	tassert.Errorf(t, reflect.DeepEqual(*lom.ObjAttrs(), before), "concurrent range GETs changed object attributes")
}

func serveOCIMetadataRange(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("ETag", ociMetadataRangeETag)
	w.Header().Set("Opc-Meta-Env", "remote")
	w.Header().Set("Content-Type", "application/x-range-test")
	w.Header().Set("Content-Length", "5")
	w.Header().Set("Content-Range", "bytes 2-6/20")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.WriteString(w, "range")
}

func checkOCIMetadataRange(t *testing.T, bp *ocibp, lom *core.LOM) {
	t.Helper()
	res := bp.GetObjReader(t.Context(), lom, 2, 5, &core.ObjPin{ETag: ociMetadataRangeETag})
	if res.Err != nil {
		t.Error(res.Err)
		return
	}
	body, readErr := io.ReadAll(res.R)
	closeErr := res.R.Close()
	tassert.CheckError(t, readErr)
	tassert.CheckError(t, closeErr)
	tassert.Errorf(t, string(body) == "range" && res.Size == 5, "range GET returned size=%d, body=%q", res.Size, body)
	tassert.Errorf(t, res.ExpCksum == nil, "range GET returned a whole-object checksum")
}

// respond to every request with 503; count requests
type retryOCITransport struct{ n atomic.Int32 }

func (s *retryOCITransport) Do(r *http.Request) (*http.Response, error) {
	s.n.Add(1)
	if r.Body != nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	body := `{"code":"ServiceUnavailable","message":"retry test"}`
	return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// client: at most cmn.BackendMaxAttempts unless OCI_SDK_DEFAULT_RETRY_ENABLED is set;
// with cmn.WithNoRetry: exactly one, regardless
func TestOCIRetryAttempts(t *testing.T) {
	orig := sdkRetryDflt
	sdkRetryDflt.noBackoff = true
	t.Cleanup(func() { sdkRetryDflt = orig })

	ops := []struct {
		name string
		call func(ctx context.Context, bp *ocibp) error
	}{
		{"head", func(ctx context.Context, bp *ocibp) error {
			_, _, err := bp.HeadObj(ctx, conditionalLOM(), nil)
			return err
		}},
		{"get", func(ctx context.Context, bp *ocibp) error {
			res := bp.GetObjReader(ctx, conditionalLOM(), 0, 16, &core.ObjPin{ETag: "etag"})
			return res.Err
		}},
		{"list", func(ctx context.Context, bp *ocibp) error {
			_, err := bp.ListObjects(ctx, conditionalLOM().Bck(), &apc.LsoMsg{}, &cmn.LsoRes{})
			return err
		}},
		{"put", func(ctx context.Context, bp *ocibp) error {
			_, err := bp.PutObj(ctx, io.NopCloser(strings.NewReader("value")), conditionalLOM(), nil)
			return err
		}},
		{"delete", func(ctx context.Context, bp *ocibp) error {
			_, err := bp.DeleteObj(ctx, conditionalLOM())
			return err
		}},
		{"put-mpu", func(ctx context.Context, bp *ocibp) error {
			bp.mpuThreshold, bp.mpuSegmentMaxSize, bp.mpuMaxThreads, bp.mpThreadPoolSize = 4, 16, 1, 1
			bp.mpChildPendingList = list.New()
			lom := conditionalLOM()
			lom.SetSize(8)
			_, err := bp.PutObj(ctx, io.NopCloser(strings.NewReader("original")), lom, nil)
			return err
		}},
		{"start-mpt", func(ctx context.Context, bp *ocibp) error {
			_, _, err := bp.StartMpt(ctx, conditionalLOM(), nil)
			return err
		}},
		{"put-mpt-part", func(ctx context.Context, bp *ocibp) error {
			_, _, err := bp.PutMptPart(ctx, conditionalLOM(), cos.NewByteReader([]byte("part")), nil, "upload", 4, 1)
			return err
		}},
		{"complete-mpt", func(ctx context.Context, bp *ocibp) error {
			_, _, _, err := bp.CompleteMpt(ctx, conditionalLOM(), nil, "upload", nil, apc.MptCompletedParts{{PartNumber: 1, ETag: "etag"}})
			return err
		}},
		{"abort-mpt", func(ctx context.Context, bp *ocibp) error {
			_, err := bp.AbortMpt(ctx, conditionalLOM(), nil, "upload")
			return err
		}},
	}
	for _, tc := range []struct {
		env          string
		wantUnmarked int32
	}{
		{"", cmn.BackendMaxAttempts},
		{"false", 1}, // operator setting takes precedence (OCI_SDK_DEFAULT_RETRY_ENABLED=false => no retries)
	} {
		t.Run("env="+tc.env, func(t *testing.T) {
			t.Setenv("OCI_SDK_DEFAULT_RETRY_ENABLED", tc.env)
			for _, op := range ops {
				for _, noRetry := range []bool{false, true} {
					bp := &ocibp{
						namespace:             "ns",
						defaultRegion:         "us-ashburn-1",
						configurationProvider: newTestOCIConfigProvider(t, "us-ashburn-1"),
						mpuThreshold:          1 << 30,
						maxPageSize:           1000,
					}
					client, _, err := bp.ociClient(nil)
					tassert.CheckFatal(t, err)
					transport := &retryOCITransport{}
					client.HTTPClient = transport
					client.Host = "http://oci.invalid"

					ctx, want := context.Background(), tc.wantUnmarked
					if noRetry {
						ctx, want = cmn.WithNoRetry(ctx), 1
					}
					err = op.call(ctx, bp)
					tassert.Fatalf(t, err != nil, "%s(no-retry=%t): expected error", op.name, noRetry)
					got := transport.n.Load()
					tassert.Fatalf(t, got == want, "%s(no-retry=%t): expected %d request(s), got %d", op.name, noRetry, want, got)
				}
			}
		})
	}
}
