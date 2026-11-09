//go:build gcp

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	coremock "github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools/tassert"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// client: at most cmn.BackendMaxAttempts; with cmn.WithNoRetry: exactly one
func TestGCPRetryAttempts(t *testing.T) {
	orig := sdkRetryDflt
	sdkRetryDflt.noBackoff = true
	t.Cleanup(func() { sdkRetryDflt = orig })

	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":{"code":503,"message":"retry test"}}`))
	}))
	t.Cleanup(srv.Close)

	client, err := gcsNewClient(context.Background(), []option.ClientOption{
		option.WithEndpoint(srv.URL + "/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(srv.Client()),
	})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { client.Close() })

	const (
		bucket = "bucket"
		key    = "obj"
	)
	ops := []struct {
		name string
		call func(ctx context.Context) error
		// SDK retries idempotent operations only
		idempotent bool
	}{
		{"head-bucket", func(ctx context.Context) error {
			_, err := gcsBucket(ctx, client, bucket).Attrs(ctx)
			return err
		}, true},
		{"head", func(ctx context.Context) error {
			_, err := gcsBucket(ctx, client, bucket).Object(key).Attrs(ctx)
			return err
		}, true},
		{"get", func(ctx context.Context) error {
			_, err := gcsBucket(ctx, client, bucket).Object(key).NewReader(ctx)
			return err
		}, true},
		{"list", func(ctx context.Context) error {
			_, err := gcsBucket(ctx, client, bucket).Objects(ctx, nil).Next()
			if err == iterator.Done {
				return nil
			}
			return err
		}, true},
		{"delete", func(ctx context.Context) error {
			return gcsBucket(ctx, client, bucket).Object(key).Delete(ctx)
		}, false},
	}
	for _, op := range ops {
		for _, noRetry := range []bool{false, true} {
			ctx, want := context.Background(), int32(1)
			if noRetry {
				ctx = cmn.WithNoRetry(ctx)
			} else if op.idempotent {
				want = cmn.BackendMaxAttempts
			}
			n.Store(0)
			err := op.call(ctx)
			tassert.Fatalf(t, err != nil, "%s(no-retry=%t): expected error", op.name, noRetry)
			got := n.Load()
			tassert.Fatalf(t, got == want, "%s(no-retry=%t): expected %d request(s), got %d", op.name, noRetry, want, got)
		}
	}
}

func TestGCPBackendPutRetry(t *testing.T) {
	for _, tc := range []struct {
		name         string
		size         int
		noRetry      bool
		failAt       int32
		status       int
		wantRequests int32
	}{
		{name: "multipart-crc", size: 1024, wantRequests: 1},
		{name: "resumable-crc", size: 17 * cos.MiB, wantRequests: 3},
		{name: "multipart-throttled", size: 1024, failAt: 1, status: http.StatusServiceUnavailable, wantRequests: 1},
		{name: "session-throttled", size: 17 * cos.MiB, failAt: 1, status: http.StatusTooManyRequests, wantRequests: 1},
		{name: "chunk-throttled", size: 17 * cos.MiB, failAt: 2, status: http.StatusServiceUnavailable, wantRequests: 2},
		{name: "chunk-throttled-on-reissue", size: 17 * cos.MiB, noRetry: true, failAt: 3, status: http.StatusServiceUnavailable, wantRequests: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := strings.Repeat("x", tc.size)
			var crc [4]byte
			binary.BigEndian.PutUint32(crc[:], crc32.Checksum([]byte(data), crc32.MakeTable(crc32.Castagnoli)))
			wantCRC := base64.StdEncoding.EncodeToString(crc[:])
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				w.Header().Set(cos.HdrContentType, "application/json")
				if r.Method != http.MethodPost || n > tc.wantRequests {
					t.Errorf("unexpected request %d: %s %s", n, r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if n == tc.failAt {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(tc.status)
					_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"retry test"}}`, tc.status)
					return
				}
				switch {
				case r.URL.Query().Get("uploadType") == "resumable":
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Location", "http://"+r.Host+"/session")
					return
				case r.URL.Path == "/session":
					_, _ = io.Copy(io.Discard, r.Body)
					if strings.HasSuffix(r.Header.Get("Content-Range"), "/*") {
						w.Header().Set("X-Http-Status-Code-Override", "308")
						return
					}
					if got := r.Header.Get("X-Goog-Hash"); got != "crc32c="+wantCRC {
						t.Errorf("final chunk checksum = %q, want crc32c=%s", got, wantCRC)
					}
				case r.URL.Query().Get("uploadType") == "multipart":
					_, params, err := mime.ParseMediaType(r.Header.Get(cos.HdrContentType))
					if err != nil {
						t.Errorf("parse multipart content type: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					part, err := multipart.NewReader(r.Body, params["boundary"]).NextPart()
					var metadata struct {
						CRC string `json:"crc32c"`
					}
					if err != nil || json.NewDecoder(part).Decode(&metadata) != nil || metadata.CRC != wantCRC {
						t.Errorf("multipart checksum = %q, want %s", metadata.CRC, wantCRC)
					}
					_, _ = io.Copy(io.Discard, r.Body)
				default:
					t.Errorf("unexpected upload request: %s", r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = fmt.Fprintf(w, `{"name":"obj","generation":"42","etag":"uploaded","crc32c":%q}`, wantCRC)
			}))
			t.Cleanup(srv.Close)

			client, err := gcsNewClient(t.Context(), []option.ClientOption{
				option.WithEndpoint(srv.URL + "/storage/v1/"),
				option.WithoutAuthentication(),
				option.WithHTTPClient(srv.Client()),
			})
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { client.Close() })
			bp := &gsbp{t: &coremock.TargetMock{}, dfltSess: gcpSess{client: client}}
			bp.dfltOnce.Do(func() {})
			lom := &core.LOM{ObjName: "obj"}
			lom.Bck().Name, lom.Bck().Provider = "bucket", apc.GCP
			ctx := t.Context()
			if tc.noRetry {
				ctx = cmn.WithNoRetry(ctx)
			}
			body := io.NopCloser(strings.NewReader(data))
			code, err := bp.PutObj(ctx, body, lom, nil)
			if tc.failAt == 0 {
				tassert.CheckFatal(t, err)
				if version := lom.ObjAttrs().Version(); version != "42" {
					t.Errorf("committed version = %q, want 42", version)
				}
			} else if code != tc.status || !cmn.IsErrTooManyRequests(err) {
				t.Errorf("PUT returned (%d, %v), want throttling error %d", code, err, tc.status)
			}
			if got := requests.Load(); got != tc.wantRequests {
				t.Errorf("got %d requests, want %d", got, tc.wantRequests)
			}
		})
	}
}
