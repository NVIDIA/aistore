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
// Object creation (plain objects, persisted metadata)
//

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
	wfh, err := cos.CreateFile(fqn)
	tassert.CheckFatal(t, err)
	if size > 0 {
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: bck.CksumConf().Type})
		tassert.CheckFatal(t, err)
		_, err = io.Copy(wfh, r)
		_ = r.Close()
		tassert.CheckFatal(t, err)
	}
	_ = wfh.Close()

	// persist metadata
	lom.SetSize(size)
	lom.IncVersion()
	lom.SetAtimeUnix(time.Now().UnixNano())
	err = lom.PersistMain(false /*chunked*/)
	tassert.CheckFatal(t, err)
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
