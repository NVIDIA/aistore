// Package res_test: unit tests
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package res_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact/xreg"
)

//
// (I) FS / config / bucket scaffolding
//

func initFS(t *testing.T, mpaths []string, targetID string) {
	t.Helper()

	fs.TestNew(nil)
	for _, mpath := range mpaths {
		err := cos.CreateDir(mpath)
		tassert.CheckFatal(t, err)

		_, err = fs.Add(mpath, targetID) // test helper from fs.go
		tassert.CheckFatal(t, err)
	}
}

func initConfig(t *testing.T, dir string, numMpaths int) *cmn.Config {
	t.Helper()

	config := cmn.GCO.BeginUpdate()
	config.ConfigDir = dir
	config.LogDir = dir
	config.TestFSP.Count = numMpaths
	config.Log.Level = "3"
	config.Disk.DiskUtilLowWM = 50
	config.Disk.DiskUtilHighWM = 80
	config.Disk.DiskUtilMaxWM = 90
	cmn.GCO.CommitUpdate(config)
	cmn.Rom.Set(&config.ClusterConfig)
	return config
}

func newBownerAndBck(name string) (meta.Bowner, *meta.Bck) {
	b := meta.NewBck(
		name, apc.AIS, cmn.NsGlobal,
		&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}},
	)
	return mock.NewBaseBownerMock(b), b
}

func newBBB(buckets ...string) (meta.Bowner, []*meta.Bck) {
	bcks := make([]*meta.Bck, 0, len(buckets))
	for _, name := range buckets {
		b := meta.NewBck(
			name, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}}, // just an example
		)
		bcks = append(bcks, b)
	}
	return mock.NewBaseBownerMock(bcks...), bcks
}

//
// (II) mountpaths and locations
//

func assertLocations(t *testing.T, bck *meta.Bck, objNames []string) {
	t.Helper()

	for _, objName := range objNames {
		uname := bck.MakeUname(objName)
		hrwMi, _, err := fs.Hrw(uname)
		tassert.CheckFatal(t, err)

		fqn := hrwMi.MakePathFQN(bck.Bucket(), fs.ObjCT, objName)
		err = cos.Stat(fqn)
		tassert.Fatalf(t, err == nil, "missing at HRW: %q (fqn=%q, err=%v)", objName, fqn, err)
	}
}

// pick any _other_ mountpath
func pickDifferentMpath(t *testing.T, notPath string) *fs.Mountpath {
	t.Helper()

	avail := fs.GetAvail()
	for p, mi := range avail {
		if p != notPath && !mi.IsAnySet(fs.FlagWaitingDD) {
			return mi
		}
	}
	tassert.CheckFatal(t, fmt.Errorf("no alternative mountpath besides %q", notPath))
	return nil
}

//
// (III) as in: PUT(mono-object), PUT(chunked-object)
//

func _writeRandFile(t *testing.T, bck *meta.Bck, fqn string, size int64) {
	wfh, err := cos.CreateFile(fqn)
	tassert.CheckFatal(t, err)
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: bck.CksumConf().Type})
	tassert.CheckFatal(t, err)
	_, err = io.Copy(wfh, r)
	_ = r.Close()
	tassert.CheckFatal(t, err)
	_ = wfh.Close()
}

func newLOM(t *testing.T, bck *meta.Bck, objName string, size int64) *core.LOM {
	t.Helper()
	var (
		wfh    *os.File
		reader readers.Reader
	)
	lom := &core.LOM{ObjName: objName}
	err := lom.InitBck(bck)
	tassert.CheckFatal(t, err)

	wfh, err = cos.CreateFile(lom.FQN)
	tassert.CheckFatal(t, err)
	defer wfh.Close()

	lom.SetSize(size)
	lom.IncVersion()
	lom.SetAtimeUnix(time.Now().UnixNano())

	if size == 0 {
		goto ret
	}

	reader, err = readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: bck.CksumConf().Type})
	tassert.CheckFatal(t, err)
	defer reader.Close()

	_, err = io.Copy(wfh, reader)
	tassert.CheckFatal(t, err)
ret:
	lom.Lock(true /*exclusive*/)
	lom.PersistMain(false /*chunked*/)
	lom.Unlock(true)
	return lom
}

func newChunkedLOM(t *testing.T, bck *meta.Bck, objName string, numChunks int, sizeChunk int64) *core.LOM {
	t.Helper()
	lom := &core.LOM{ObjName: objName}
	err := lom.InitBck(bck)
	tassert.CheckFatal(t, err)

	totalSize := int64(numChunks) * sizeChunk
	lom.SetSize(totalSize)

	// Create Ufest for chunked upload
	ufest, err := core.NewUfest("", lom, false)
	tassert.CheckFatal(t, err)

	// Create chunks
	for i := 1; i <= numChunks; i++ {
		chunk, err := ufest.NewChunk(i, lom)
		tassert.CheckFatal(t, err)

		_writeRandFile(t, bck, chunk.Path(), sizeChunk)

		err = ufest.Add(chunk, sizeChunk, int64(i))
		tassert.CheckFatal(t, err)
	}

	err = lom.CompleteUfest(ufest, false)
	tassert.CheckFatal(t, err)

	err = lom.Load(false, false)
	tassert.CheckFatal(t, err)

	tassert.Fatal(t, lom.IsChunked(), "expecting lom _chunked_ upon loading")
	return lom
}

func createPlainObjectAt(t *testing.T, bck *meta.Bck, mi *fs.Mountpath, objName string, size int64) {
	t.Helper()

	fqn := mi.MakePathFQN(bck.Bucket(), fs.ObjCT, objName)
	err := cos.CreateDir(filepath.Dir(fqn))
	tassert.CheckFatal(t, err)

	// init LOM bound to that fqn
	lom := &core.LOM{ObjName: objName}
	err = lom.InitFQN(fqn, bck.Bucket())
	tassert.CheckFatal(t, err)

	// lock to match normal invariants for create+persist
	lom.Lock(true)
	defer lom.Unlock(true)

	// write payload
	if size > 0 {
		_writeRandFile(t, bck, fqn, size)
	}

	// persist metadata
	lom.SetSize(size)
	lom.IncVersion()
	lom.SetAtimeUnix(time.Now().UnixNano())
	err = lom.PersistMain(false /*chunked*/)
	tassert.CheckFatal(t, err)
}

func createChunkedLOMAt(t *testing.T, bck *meta.Bck, mi *fs.Mountpath, objName string, numChunks int, sizeChunk int64) *core.LOM {
	t.Helper()

	fqn := mi.MakePathFQN(bck.Bucket(), fs.ObjCT, objName)
	err := cos.CreateDir(filepath.Dir(fqn))
	tassert.CheckFatal(t, err)

	lom := &core.LOM{ObjName: objName}
	err = lom.InitFQN(fqn, bck.Bucket())
	tassert.CheckFatal(t, err)

	totalSize := int64(numChunks) * sizeChunk
	lom.SetSize(totalSize)

	ufest, err := core.NewUfest("", lom, false)
	tassert.CheckFatal(t, err)

	for i := 1; i <= numChunks; i++ {
		chunk, err := ufest.NewChunk(i, lom)
		tassert.CheckFatal(t, err)

		_writeRandFile(t, bck, chunk.Path(), sizeChunk)

		err = ufest.Add(chunk, sizeChunk, int64(i))
		tassert.CheckFatal(t, err)
	}

	err = lom.CompleteUfest(ufest, false)
	tassert.CheckFatal(t, err)

	lom.UncacheUnless()
	err = lom.Load(false, false)
	tassert.CheckFatal(t, err)

	tassert.Fatal(t, lom.IsChunked(), "expecting lom _chunked_ upon loading")
	return lom
}

//
// (IV) Wait helpers (keep tight; avoid random sleeps).
//

func waitNonEmptyXid(t *testing.T, r *res.Res, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if xid := r.CurrentXactID(); xid != "" {
			return xid
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatal("timeout waiting for CurrentXactID() to become non-empty")
	return ""
}

func waitProgressBySnap(t *testing.T, xid string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		xctn, err := xreg.GetXact(xid)
		tassert.CheckFatal(t, err)
		if xctn != nil {
			s := xctn.Snap()
			if s.Stats.Objs > 0 || s.Stats.Bytes > 0 {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for progress (xid=%q)", xid)
}

func waitAborted(t *testing.T, xid string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		xctn, err := xreg.GetXact(xid)
		tassert.CheckFatal(t, err)
		if xctn != nil && xctn.IsAborted() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for abort (xid=%q)", xid)
}

func waitDone(t *testing.T, xid string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		xctn, err := xreg.GetXact(xid)
		tassert.CheckFatal(t, err)
		if xctn != nil && xctn.IsDone() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting done (xid=%q)", xid)
}
