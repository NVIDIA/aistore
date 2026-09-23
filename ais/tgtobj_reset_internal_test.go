// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const coldGetResetContent = "replacement fetched after eviction\n"

type coldGetResetBackend struct {
	core.Backend
	oldBMD      *bucketMD
	oldBackends backends
	objName     string
	calls       int
}

func (*coldGetResetBackend) MetricName(name string) string { return name }

func (bp *coldGetResetBackend) restore() {
	mockTarget.bps = bp.oldBackends
	mockTarget.owner.bmd.put(bp.oldBMD)
}

func (bp *coldGetResetBackend) GetObjReader(_ context.Context, lom *core.LOM, _, _ int64) core.GetReaderResult {
	bp.calls++
	if lom.ObjName != bp.objName {
		return core.GetReaderResult{Err: fmt.Errorf("backend GET used %q, expected %q", lom.ObjName, bp.objName), ErrCode: http.StatusNotFound}
	}
	// Observe the production Reset call before the backend supplies new metadata.
	if lom.IsChunked(true) || lom.IsFntl() || lom.HasShardIdx() || lom.Checksum() != nil ||
		lom.Lsize(true) != 0 || lom.VersionPtr() != nil || lom.GetCustomMD() != nil {
		return core.GetReaderResult{Err: fmt.Errorf("cold GET retained stale metadata for %q", bp.objName), ErrCode: http.StatusInternalServerError}
	}
	return core.GetReaderResult{R: io.NopCloser(strings.NewReader(coldGetResetContent)), Size: int64(len(coldGetResetContent))}
}

func TestGetObjectColdMissReset(t *testing.T) {
	bp := &coldGetResetBackend{oldBMD: mockTarget.owner.bmd.get(), oldBackends: mockTarget.bps}
	t.Cleanup(bp.restore)
	mockTarget.bps = backends{apc.AIS: bp}
	bck := meta.NewBck("cold-get-reset", apc.AIS, cmn.Ns{UUID: "remote"})
	bmd := bp.oldBMD.clone()
	bmd.add(bck, &cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}})
	mockTarget.owner.bmd.put(bmd)
	for _, err := range fs.CreateBucket(bck.Bucket(), false /*nilbmd*/) {
		tassert.CheckFatal(t, err)
	}

	for _, objName := range []string{"chunked", strings.Repeat("x", 280)} {
		t.Logf("cold GET after eviction: %q", objName)
		bp.objName, bp.calls = objName, 0
		lom := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, lom.InitBck(bck))
		originalFQN := lom.FQN

		// Cache two chunks and retain the loaded handle across eviction.
		const chunkSize = 32 * cos.KiB
		source, err := readers.New(&readers.Arg{Type: readers.Rand, Size: 2 * chunkSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		poi := putOI{
			t: mockTarget, lom: lom, r: source, size: 2 * chunkSize,
			oreq: httptest.NewRequest(http.MethodGet, "/", http.NoBody), skipBackend: true, locked: true,
		}
		lom.Lock(true)
		_, err = poi.chunk(chunkSize)
		lom.Unlock(true)

		// TODO:
		// rand-reader.Close() always returns nil
		// but here the source.Close() would be expected to return an error on second close

		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, lom.Load(false, false))
		tassert.Fatalf(t, lom.IsChunked(), "expected initially chunked object")
		if fs.IsFntl(objName) {
			tassert.Fatalf(t, lom.IsFntl() && lom.ObjName != objName, "expected shortened local name")
		}

		remover := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, remover.InitBck(bck))
		_, err = mockTarget.EvictObject(remover)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, cos.IsNotExist(cos.Stat(lom.FQN)), "eviction did not remove the local file")
		tassert.Fatalf(t, lom.IsChunked(), "test requires the original handle to retain stale metadata")

		// Do not call Reset here: get() must reset on the miss and run cold PUT itself.
		w := httptest.NewRecorder()
		goi := getOI{t: mockTarget, lom: lom, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
		_, err = goi.getObject()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, goi.cold && bp.calls == 1, "expected exactly one cold GET, got %d backend reads", bp.calls)
		tassert.Fatalf(t, w.Code == http.StatusOK && w.Body.String() == coldGetResetContent, "incorrect cold GET response")
		tassert.Fatalf(t, lom.IsLocked() == apc.LockNone, "cold GET leaked the object lock")

		// A fresh handle must discover the monolithic replacement, including FNTL metadata.
		fresh := &core.LOM{ObjName: objName}
		tassert.CheckFatal(t, fresh.InitBck(bck))
		fresh.Lock(false)
		fresh.Uncache() // verify persistence, not just the metadata cache
		err = fresh.Load(false, true)
		fresh.Unlock(false)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !fresh.IsChunked() && fresh.Lsize() == int64(len(coldGetResetContent)), "invalid replacement metadata")
		if fs.IsFntl(objName) {
			tassert.Fatalf(t, fresh.IsFntl() && fresh.ObjName != objName, "cold PUT did not shorten the name again")
			tassert.Fatalf(t, fresh.OrigFntl()[0] == originalFQN && fresh.OrigFntl()[1] == objName, "lost original-name mapping")
		}
		w = httptest.NewRecorder()
		goi = getOI{t: mockTarget, lom: fresh, w: w, dpq: &dpq{}, ctx: t.Context(), atime: time.Now().UnixNano()}
		_, err = goi.getObject()
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !goi.cold && bp.calls == 1, "warm GET unexpectedly contacted the backend")
		tassert.Fatalf(t, w.Body.String() == coldGetResetContent, "incorrect warm GET response")
		tassert.Fatalf(t, fresh.IsLocked() == apc.LockNone, "warm GET leaked the object lock")
		_, err = mockTarget.EvictObject(fresh)
		tassert.CheckFatal(t, err)
	}
}
