// Package res_test: unit tests
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package res_test

import (
	"fmt"
	"io"
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
	"github.com/NVIDIA/aistore/xact/xs"
)

// Sequence: BeginDD (mark waitingDD, still walkable) => resilver => finalize Disable => Enable => resilver again
// namely:
//  1. create N mountpaths
//  2. create many plain objects (some intentionally misplaced)
//  3. disable a mountpath
//  4. RunResilver -> verify objects at *current* HRW
//  5. re-enable mountpath
//  6. RunResilver -> verify again
func TestResilver_DisableEnable(t *testing.T) {
	const (
		numMpaths = 5
		numObjs   = 80
		bucket    = "ut-resilver-bck"
	)

	// BEGIN init target  -----------------------------------------------------------
	bowner, bck := newBownerAndBck(bucket)
	target := mock.NewTarget(bowner)

	tmpDir := t.TempDir()

	mpaths := make([]string, 0, numMpaths)
	for i := range numMpaths {
		mpaths = append(mpaths, filepath.Join(tmpDir, fmt.Sprintf("mpath-%d", i)))
	}

	initFS(t, mpaths, target.SID())
	config := initConfig(t, tmpDir, numMpaths)
	xreg.Init()
	xs.Treg(nil)
	// END init target --------------------------------------------------------------

	// create objects: half on non-HRW mountpath to force relocation
	rng := cos.NowRand()
	objs := make([]string, 0, numObjs)
	for i := range numObjs {
		objName := fmt.Sprintf("a/b/c/obj-%04d", i)
		objs = append(objs, objName)

		// HRW for this object under current availability
		uname := bck.MakeUname(objName)
		hrwMi, _, err := fs.Hrw(uname)
		tassert.CheckFatal(t, err)

		// intentionally misplace every other object
		dstMi := hrwMi
		if i%2 == 1 {
			dstMi = pickDifferentMpath(t, hrwMi.Path)
		}
		createPlainObjectAt(t, bck, dstMi, objName, int64(1024+rng.IntN(4096)))
	}

	// choose one mountpath to disable (deterministic)
	mpathToToggle := mpaths[2]

	// begin dd (mark waitingDD, still in avail)
	flags := fs.FlagWaitingDD
	rmi, _, _, err := fs.BeginDD(apc.ActMountpathDisable, flags, mpathToToggle)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, rmi != nil, "BeginDD returned nil mi for %q", mpathToToggle)

	// resilver and check
	runResilver(t, config, rmi, apc.ActMountpathDisable)
	assertAllAtCurrentHRW(t, bck, objs)

	// disable (as in: commit)
	disabledMi, err := fs.Disable(mpathToToggle)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMi != nil, "expected disabled mountpath, got nil: %q", mpathToToggle)

	// re-enable, resilver, and check again
	enabledMi, err := fs.Enable(mpathToToggle)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabledMi != nil, "expected enabled mountpath, got nil: %q", mpathToToggle)

	runResilver(t, config, disabledMi, apc.ActMountpathEnable)
	assertAllAtCurrentHRW(t, bck, objs)
}

func TestUfestRelocate_ChunkedOne(t *testing.T) {
	const (
		numMpaths = 3
		bucket    = "ut-relocate-chunked"
		objName   = "a/b/c/chunked-obj"
		numChunks = 10
		chunkSize = int64(16 * cos.KiB)
	)

	// init target + fs
	bowner, bck := newBownerAndBck(bucket)
	target := mock.NewTarget(bowner)
	tmpDir := t.TempDir()

	mpaths := make([]string, 0, numMpaths)
	for i := range numMpaths {
		mpaths = append(mpaths, filepath.Join(tmpDir, fmt.Sprintf("mpath-%d", i)))
	}

	initFS(t, mpaths, target.SID())
	_ = initConfig(t, tmpDir, numMpaths)

	// compute HRW mountpath for the object
	uname := bck.MakeUname(objName)
	hrwMi, _, err := fs.Hrw(uname)
	tassert.CheckFatal(t, err)

	// pick a different mountpath to intentionally misplace the object (chunk #1)
	srcMi := pickDifferentMpath(t, hrwMi.Path)
	tassert.Fatalf(t, srcMi != nil && srcMi.Path != hrwMi.Path, "setup failed: srcMi == hrwMi (%v)", srcMi)

	// create a completed chunked object on the wrong mountpath
	lom := createChunkedLOMAt(t, bck, srcMi, objName, numChunks, chunkSize)

	// load completed manifest
	lom.Lock(true) // --------------------------------- locked
	u, err := core.NewUfest("", lom, true)
	tassert.CheckFatal(t, err)
	err = u.LoadCompleted(lom)
	tassert.CheckFatal(t, err)

	// capture old manifest fqn
	// (should be removed after successful relocation when chunk #1 moves)
	oldManifestFQN := lom.GenFQN(fs.ChunkMetaCT)

	// relocate
	buf := make([]byte, 128*cos.KiB)
	hlom, err := u.Relocate(hrwMi, buf)
	lom.Unlock(true) // --------------------------------- unlocked

	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, hlom != nil, "expected non-nil hlom")

	// new canonical FQN for chunk #1
	dstObjFQN := hrwMi.MakePathFQN(bck.Bucket(), fs.ObjCT, objName)

	// assert returned LOM points to HRW
	tassert.Fatalf(t, hlom.FQN == dstObjFQN, "hlom fqn mismatch: have %q, want %q", hlom.FQN, dstObjFQN)
	tassert.Fatalf(t, hlom.Mountpath().Path == hrwMi.Path, "hlom mountpath mismatch: have %q, want %q", hlom.Mountpath().Path, hrwMi.Path)

	// chunk #1 exists at HRW
	tassert.CheckFatal(t, cos.Stat(dstObjFQN))

	// old chunk #1 path should be gone (moved, not copied)
	oldObjFQN := srcMi.MakePathFQN(bck.Bucket(), fs.ObjCT, objName)
	err = cos.Stat(oldObjFQN)
	tassert.Fatalf(t, err != nil, "expected old object fqn to be gone: %q", oldObjFQN)

	// completed manifest exists at new location
	newManifestFQN := hlom.GenFQN(fs.ChunkMetaCT)
	tassert.CheckFatal(t, cos.Stat(newManifestFQN))

	// old completed manifest removed (best-effort; expect not-exist on success path)
	err = cos.Stat(oldManifestFQN)
	tassert.Fatalf(t, err != nil, "expected old manifest to be removed: %q", oldManifestFQN)

	// re-validate relocated (chunked) hlom
	hlom.Lock(true) // --------------------------------- locked
	u, err = core.NewUfest("", hlom, true)
	tassert.CheckFatal(t, err)
	err = u.LoadCompleted(hlom)
	tassert.CheckFatal(t, err)
	err = u.ValidateChunkLocations(true /*completed*/)
	tassert.CheckFatal(t, err)
	hlom.Unlock(true) // --------------------------------- unlocked
}

//
// Resilver runner
//

func runResilver(t *testing.T, config *cmn.Config, rmi *fs.Mountpath, action string) {
	t.Helper()

	r := res.New()
	args := &res.Args{
		UUID:   cos.GenUUID(),
		Rmi:    rmi,
		Action: action,
		Custom: xreg.ResArgs{
			Config: config,
			Smap:   nil, // TODO: check if Smap is ever needed (in re: SkipGobMisplaced)
		},
		SingleRmiJogger: false,
	}

	tstats := mock.NewStatsTracker()
	r.RunResilver(args, tstats)
}

//
// Assertions
//

func assertAllAtCurrentHRW(t *testing.T, bck *meta.Bck, objNames []string) {
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

//
// Object creation (plain and chunked)
//

func createTestFile(t *testing.T, bck *meta.Bck, fqn string, size int64) {
	wfh, err := cos.CreateFile(fqn)
	tassert.CheckFatal(t, err)
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: bck.CksumConf().Type})
	tassert.CheckFatal(t, err)
	_, err = io.Copy(wfh, r)
	_ = r.Close()
	tassert.CheckFatal(t, err)
	_ = wfh.Close()
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
		createTestFile(t, bck, fqn, size)
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

		createTestFile(t, bck, chunk.Path(), sizeChunk)

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
// FS / config / bucket scaffolding
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
