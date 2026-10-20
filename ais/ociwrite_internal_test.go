// Package ais: internal tests.
// no-copyright
package ais

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

type orderedOCIBackend struct {
	core.Backend
	client  *http.Client
	url     string
	mu      sync.Mutex
	uploads map[string][]byte
	next    atomic.Int32
}

func (*orderedOCIBackend) Provider() string              { return apc.OCI }
func (*orderedOCIBackend) MetricName(name string) string { return name }
func (bp *orderedOCIBackend) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	defer r.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, bp.url, r)
	if err != nil {
		return 0, err
	}
	resp, err := bp.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	if err == nil {
		lom.SetCustomKey(cmn.ETag, resp.Header.Get("ETag"))
	}
	return 0, err
}
func (bp *orderedOCIBackend) GetObjReader(ctx context.Context, _ *core.LOM, _, _ int64) core.GetReaderResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bp.url, http.NoBody)
	if err != nil {
		return core.GetReaderResult{Err: err}
	}
	resp, err := bp.client.Do(req)
	if err != nil {
		return core.GetReaderResult{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return core.GetReaderResult{R: io.NopCloser(bytes.NewReader(data)), Size: int64(len(data)), Err: err}
}
func (bp *orderedOCIBackend) StartMpt(context.Context, *core.LOM, *http.Request) (string, int, error) {
	id := fmt.Sprintf("ordering-upload-%d", bp.next.Add(1))
	bp.mu.Lock()
	bp.uploads[id] = nil
	bp.mu.Unlock()
	return id, 0, nil
}
func (bp *orderedOCIBackend) PutMptPart(_ context.Context, _ *core.LOM, r cos.ReadOpenCloser, _ *http.Request, id string, _ int64, part int32) (string, int, error) {
	data, err := io.ReadAll(r)
	bp.mu.Lock()
	bp.uploads[id] = append(bp.uploads[id], data...)
	bp.mu.Unlock()
	return fmt.Sprintf("part-%d", part), 0, err
}
func (bp *orderedOCIBackend) CompleteMpt(ctx context.Context, lom *core.LOM, r *http.Request, id string, _ []byte, _ apc.MptCompletedParts) (string, string, int, error) {
	bp.mu.Lock()
	data := bytes.Clone(bp.uploads[id])
	delete(bp.uploads, id)
	bp.mu.Unlock()
	code, err := bp.PutObj(ctx, io.NopCloser(bytes.NewReader(data)), lom, r)
	etag, _ := lom.GetCustomKey(cmn.ETag)
	return "", etag, code, err
}

func TestOCIOverlappingCachePublication(t *testing.T) {
	for _, oldMultipart := range []bool{false, true} {
		for _, newMultipart := range []bool{false, true} {
			t.Run(fmt.Sprintf("old-multipart-%t/new-multipart-%t", oldMultipart, newMultipart), func(t *testing.T) {
				committed := make(chan struct{})
				release := make(chan struct{})
				var releaseOnce sync.Once
				var remoteMu sync.Mutex
				var remote []byte
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet {
						remoteMu.Lock()
						data := bytes.Clone(remote)
						remoteMu.Unlock()
						_, _ = w.Write(data)
						return
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					remoteMu.Lock()
					remote = bytes.Clone(data)
					remoteMu.Unlock()
					w.Header().Set("ETag", string(data))
					if string(data) == "older" {
						close(committed)
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
				}))
				defer srv.Close()
				defer releaseOnce.Do(func() { close(release) })
				bp := &orderedOCIBackend{client: &http.Client{Timeout: 10 * time.Second}, url: srv.URL, uploads: make(map[string][]byte)}
				oldBackends, oldBMD := mockTarget.bps, mockTarget.owner.bmd.get()
				defer func() { mockTarget.bps = oldBackends; mockTarget.owner.bmd.put(oldBMD) }()
				previousConfig := cmn.GCO.Get()
				config := cmn.GCO.BeginUpdate()
				config.Backend.Providers = maps.Clone(config.Backend.Providers)
				if config.Backend.Providers == nil {
					config.Backend.Providers = make(map[string]cmn.Ns)
				}
				config.Backend.Providers[apc.OCI] = cmn.NsGlobal
				cmn.GCO.CommitUpdate(config)
				defer cmn.GCO.Put(previousConfig)
				var writers sync.WaitGroup
				defer func() { releaseOnce.Do(func() { close(release) }); writers.Wait() }()
				mockTarget.bps = backends{apc.OCI: bp}
				bck := meta.NewBck(fmt.Sprintf("oci-cache-order-%t-%t-%s", oldMultipart, newMultipart, trand.String(8)), apc.OCI, cmn.NsGlobal)
				bmd := oldBMD.clone()
				bmd.add(bck, &cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}})
				mockTarget.owner.bmd.put(bmd)
				for _, err := range fs.CreateBucket(bck.Bucket(), false) {
					tassert.CheckFatal(t, err)
				}
				put := func(data string, multipart bool) error {
					lom := core.AllocLOM("manifest")
					defer core.FreeLOM(lom)
					if err := lom.InitBck(bck); err != nil {
						return err
					}
					if multipart {
						poi := putOI{t: mockTarget, lom: lom, r: io.NopCloser(strings.NewReader(data)), size: int64(len(data)), config: cmn.GCO.Get(), oreq: httptest.NewRequest(http.MethodPut, "/", http.NoBody)}
						_, err := poi.chunk(3)
						return err
					}
					return mockTarget.PutObject(lom, &core.PutParams{Reader: cos.NewByteReader([]byte(data)), WorkTag: fs.WorkfilePut, Atime: time.Now(), Size: int64(len(data)), OWT: cmn.OwtPut})
				}
				olderDone := make(chan error, 1)
				writers.Go(func() { olderDone <- put("older", oldMultipart) })
				select {
				case <-committed:
				case err := <-olderDone:
					t.Fatalf("older write failed before committing: %v", err)
				case <-time.After(10 * time.Second):
					t.Fatal("older write did not commit")
				}
				newerDone := make(chan error, 1)
				writers.Go(func() { newerDone <- put("newer", newMultipart) })
				select {
				case err := <-newerDone:
					tassert.CheckFatal(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("backend request held an object lock")
				}
				releaseOnce.Do(func() { close(release) })
				tassert.CheckFatal(t, <-olderDone)
				lom := core.AllocLOM("manifest")
				defer core.FreeLOM(lom)
				tassert.CheckFatal(t, lom.InitBck(bck))
				w := httptest.NewRecorder()
				goi := getOI{t: mockTarget, lom: lom, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
				_, err := goi.getObject()
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, w.Body.String() == "newer", "cache published %q after newer backend commit", w.Body.String())
				tassert.Fatalf(t, goi.cold, "overlapping writes must discard uncertain cached data")
				tassert.CheckFatal(t, put("final", false))
				warm := core.AllocLOM("manifest")
				defer core.FreeLOM(warm)
				tassert.CheckFatal(t, warm.InitBck(bck))
				w = httptest.NewRecorder()
				goi = getOI{t: mockTarget, lom: warm, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
				_, err = goi.getObject()
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, !goi.cold && w.Body.String() == "final", "uncontended write lost its warm cache")
				tassert.Fatalf(t, len(mockTarget.ociWrites.pending) == 0, "write bookkeeping leaked")
			})
		}
	}
}
