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
	"hash/crc32"
	"io"
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
		name    string
		size    int
		noRetry bool
		status  int
	}{
		{name: "committed-metadata", size: 1024, status: http.StatusOK},
		{name: "single-attempt", size: 17 * cos.MiB, noRetry: true, status: http.StatusServiceUnavailable},
		// unconditional (non-idempotent) writes: no SDK retries; > default ChunkSize: still a single request
		{name: "single-request", size: 17 * cos.MiB, status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set(cos.HdrContentType, "application/json")
				if r.Method != http.MethodPost || r.URL.Query().Get("uploadType") != "multipart" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(tc.status)
				if tc.status == http.StatusOK {
					// single-request upload: the SDK validates CRC32C (Castagnoli, big-endian, base64)
					var crc [4]byte
					binary.BigEndian.PutUint32(crc[:], crc32.Checksum([]byte(strings.Repeat("x", tc.size)), crc32.MakeTable(crc32.Castagnoli)))
					_, _ = io.WriteString(w, `{"name":"obj","generation":"42","etag":"uploaded","crc32c":"`+
						base64.StdEncoding.EncodeToString(crc[:])+`"}`)
				} else {
					_, _ = io.WriteString(w, `{"error":{"code":503,"message":"retry test"}}`)
				}
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
			body := io.NopCloser(strings.NewReader(strings.Repeat("x", tc.size)))
			code, err := bp.PutObj(ctx, body, lom, nil)
			if tc.status == http.StatusOK {
				tassert.CheckFatal(t, err)
				if version := lom.ObjAttrs().Version(); version != "42" {
					t.Errorf("committed version = %q, want 42", version)
				}
			} else if code != tc.status || !cmn.IsErrTooManyRequests(err) {
				t.Errorf("PUT returned (%d, %v), want throttling error %d", code, err, tc.status)
			}
			if got := requests.Load(); got != 1 {
				t.Errorf("got %d requests, want 1", got)
			}
		})
	}
}
