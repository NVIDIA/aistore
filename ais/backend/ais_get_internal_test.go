// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

type aisGetTarget struct {
	core.TargetPut
	put func(*core.LOM, *core.PutParams) error
}

func (t *aisGetTarget) PutObject(lom *core.LOM, params *core.PutParams) error {
	return t.put(lom, params)
}

func TestAISGetObjAttributes(t *testing.T) {
	const body = "remote AIS object"
	writeErr := errors.New("local write failed")
	for _, tc := range []struct {
		name, checksum string
		malformed      bool
		writeErr       error
	}{
		{name: "xxhash", checksum: cos.ChecksumOneXxh},
		{name: "md5", checksum: cos.ChecksumMD5},
		{name: "no-checksum"},
		{name: "malformed-metadata", malformed: true},
		{name: "write-error", checksum: cos.ChecksumOneXxh, writeErr: writeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet || r.Header.Get(cos.HdrRange) != "" {
					t.Errorf("expected full GET, got %s, range %q", r.Method, r.Header.Get(cos.HdrRange))
				}
				w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(body)))
				w.Header().Set(apc.HdrObjVersion, "7")
				w.Header().Set(apc.HdrObjCustomMD, "key=value")
				if tc.checksum != "" {
					w.Header().Set(apc.HdrObjCksumType, tc.checksum)
					w.Header().Set(apc.HdrObjCksumVal, cos.ChecksumB2S([]byte(body), tc.checksum))
				}
				if tc.malformed {
					w.Header().Set(apc.HdrObjCustomMD, "missing-separator")
				}
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(server.Close)
			lom := &core.LOM{ObjName: "object"}
			lom.Bck().Name, lom.Bck().Provider, lom.Bck().Ns.UUID = "bucket", apc.AIS, "remote-uuid"
			lom.SetCustomMD(cos.StrKVs{"stale": "discard"})
			called := false
			target := &aisGetTarget{put: func(got *core.LOM, params *core.PutParams) error {
				called = true
				defer params.Reader.Close()
				if got != lom || params.Size != int64(len(body)) || params.OWT != cmn.OwtGetPrefetchLock ||
					!params.SkipBackend || params.WorkTag != fs.WorkfileColdget || params.Atime.IsZero() {
					t.Errorf("unexpected PUT parameters: %+v", params)
				}
				if tc.checksum == "" {
					if !cos.NoneC(params.Cksum) {
						t.Errorf("unexpected checksum: %v", params.Cksum)
					}
				} else if params.Cksum == nil || params.Cksum.Type() != tc.checksum ||
					params.Cksum.Value() != cos.ChecksumB2S([]byte(body), tc.checksum) {
					t.Errorf("expected remote %s checksum, got %v", tc.checksum, params.Cksum)
				}
				md := got.GetCustomMD()
				if got.ObjAttrs().Version() != "7" || md["key"] != "value" ||
					md[cmn.SourceObjMD] != apc.AIS || md["stale"] != "" {
					t.Errorf("GET metadata was not applied: %+v", got.ObjAttrs())
				}
				data, err := io.ReadAll(params.Reader)
				if err != nil || string(data) != body {
					t.Errorf("body %q, err %v", data, err)
				}
				return tc.writeErr
			}}
			bp := &AISbp{t: target, remote: map[string]*remAis{
				"remote-uuid": {uuid: "remote-uuid", bpL: api.BaseParams{URL: server.URL, Client: server.Client()}},
			}}
			code, err := bp.GetObj(t.Context(), lom, cmn.OwtGetPrefetchLock, nil)
			if tc.malformed {
				if code != http.StatusBadGateway || err == nil || called {
					t.Fatalf("malformed metadata: code=%d, err=%v, put=%t", code, err, called)
				}
			} else if !called || !errors.Is(err, tc.writeErr) || code != 0 {
				t.Fatalf("code=%d, err=%v, put=%t", code, err, called)
			}
			if requests.Load() != 1 {
				t.Errorf("expected one GET, got %d requests", requests.Load())
			}
		})
	}
}
