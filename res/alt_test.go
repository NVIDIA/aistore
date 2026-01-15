// Package res_test: unit tests
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package res_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
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

	time.Sleep(time.Second) // TODO -- FIXME: remove (a rare, intermittent failure)

	assertLocations(t, bck, objs)

	// disable (as in: commit)
	disabledMi, err := fs.Disable(mpathToToggle)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMi != nil, "expected disabled mountpath, got nil: %q", mpathToToggle)

	// re-enable, resilver, and check again
	enabledMi, err := fs.Enable(mpathToToggle)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabledMi != nil, "expected enabled mountpath, got nil: %q", mpathToToggle)

	runResilver(t, config, disabledMi, apc.ActMountpathEnable)

	time.Sleep(time.Second) // TODO -- FIXME: remove (a rare, intermittent failure)

	assertLocations(t, bck, objs)
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
	r.Run(args, tstats)
}

// This test validates:
// - resilver starts under target's volume change
// - mid-flight abort works
// - a subsequent resilver run completes
// - objects are present at HRW under the final volume.
func TestResilver_PreemptAddDisable(t *testing.T) {
	const (
		numBaseMpaths  = 3
		numExtraMpaths = 1
		maxObjects     = 10000
		plainObjSize   = cos.KiB
		numChunkedObjs = 2000
		numChunks      = 8
		chunkSize      = 13 * cos.KiB
		waitStartTO    = 2 * time.Second
		waitProgressTO = 10 * time.Second
		waitAbortTO    = 10 * time.Second
		waitDoneTO     = 60 * time.Second

		// ensure we actually have work after volume change
		minRelocations = (maxObjects + numChunkedObjs) / (numBaseMpaths + 1) / 2
	)
	debug.Assert(minRelocations > 50)

	const bucket = "ut-resilver-preempt"

	// init: target + config + xreg
	bowner, bcks := newBBB(bucket) // helper from res/unit_test.go
	_ = mock.NewTarget(bowner)

	tmpDir := t.TempDir()
	targetID := "t1" // stable test id; fs.Add wants one

	// prepare mountpaths
	base := make([]string, 0, numBaseMpaths+numExtraMpaths)
	for i := range numBaseMpaths + numExtraMpaths {
		base = append(base, filepath.Join(tmpDir, fmt.Sprintf("mpath-%d", i)))
	}

	// start with only the base mountpaths registered (volume A)
	initFS(t, base[:numBaseMpaths], targetID)      // helper from res/unit_test.go
	config := initConfig(t, tmpDir, numBaseMpaths) // helper from res/unit_test.go
	xreg.Init()
	xs.Treg(nil)

	bck := bcks[0]

	// create objects on volume A
	objNames := make([]string, 0, 2048)
	createdAt := make(map[string]string, 2048) // obj -> mountpath path (under A)

	// plain objects
	for i := range maxObjects {
		obj := fmt.Sprintf("a/b/c/plain-%06d", i)
		lom := newLOM(t, bck, obj, plainObjSize) // canonical HRW under A
		objNames = append(objNames, obj)
		createdAt[obj] = lom.Mountpath().Path
	}

	// chunked objects (also canonical under A)
	for i := range numChunkedObjs {
		obj := fmt.Sprintf("a/b/c/chunked-%03d", i)
		lom := newChunkedLOM(t, bck, obj, numChunks, chunkSize)
		objNames = append(objNames, obj)
		createdAt[obj] = lom.Mountpath().Path
	}

	// Add an extra mountpath (volume B)
	tlog.Logln("1. Add an extra mountpath (volume B)")
	extraMpath := base[numBaseMpaths]
	tassert.CheckFatal(t, cos.CreateDir(extraMpath))
	_, err := fs.Add(extraMpath, targetID)
	tassert.CheckFatal(t, err)

	// ensure we have enough names that changed location on volume B
	relocations := 0
	for _, obj := range objNames {
		uname := bck.MakeUname(obj)
		hrwMi, _, err := fs.Hrw(uname)
		tassert.CheckFatal(t, err)
		if createdAt[obj] != hrwMi.Path {
			relocations++
		}
	}
	tassert.Fatalf(t, relocations >= minRelocations,
		"insufficient location changes after adding mpath: have %d, want >= %d (consider increasing object count)",
		relocations, minRelocations)

	tlog.Logln("2. Start resilver #1")
	r := res.New()
	tstats := mock.NewStatsTracker()

	// start resilver A (under volume B) in a goroutine; use WG barrier
	wgA := &sync.WaitGroup{}
	wgA.Add(1)
	uuidA := cos.GenUUID()
	argsA := &res.Args{
		UUID:   uuidA,
		WG:     wgA,
		Action: apc.ActMountpathAttach,
		Custom: xreg.ResArgs{Config: config},
	}
	go r.Run(argsA, tstats)

	// wait until initRenew done and xaction is registered/owned
	wgA.Wait()
	xidA := uuidA
	tlog.Logfln("3. Resilver #1 %q is now running", xidA)

	// ensure A is actually doing work before preempting (avoid “aborted before first visit”)
	waitProgressBySnap(t, xidA, waitProgressTO)

	// Disable the extra mountpath to revert volume
	flags := fs.FlagWaitingDD
	rmi, _, _, err := fs.BeginDD(apc.ActMountpathDisable, flags, extraMpath)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, rmi != nil, "BeginDD returned nil mi for %q", extraMpath)

	// start resilver B (under volume C) in a goroutine; WG barrier again
	tlog.Logln("5. Start resilver #2")
	wgB := &sync.WaitGroup{}
	wgB.Add(1)
	uuidB := cos.GenUUID()
	argsB := &res.Args{
		UUID:   uuidB,
		WG:     wgB,
		Action: apc.ActMountpathDisable,
		Custom: xreg.ResArgs{Config: config},
	}

	go r.Run(argsB, tstats)

	// verify A aborted (via xreg)
	waitLikelyAborted(t, xidA, waitAbortTO)

	xidB := uuidB
	tlog.Logfln("6. Resilver #2 %q is now running", xidB)

	// wait for B to complete (via xreg)
	waitDone(t, xidB, waitDoneTO)

	_, err = fs.Disable(extraMpath)
	tassert.CheckFatal(t, err)

	// all objects must exist on volume C
	assertLocations(t, bck, objNames)

	tlog.Logln("7. Done")
}
