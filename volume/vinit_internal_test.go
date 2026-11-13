// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRestoreDisabledMountpath(t *testing.T) {
	const tid = "target01"

	root := t.TempDir()
	live, err := fs.NewMountpath(root, "")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "disabled")
	vmd := newVMD(1)
	vmd.DaemonID = tid
	vmd.Mountpaths[path] = &fsMpathMD{
		Path:   path,
		Fs:     live.Fs,
		FsType: live.FsType,
		FsID:   live.FsID,
	}

	fs.NewTestMFS(mock.NewIOS())
	if err := initMPI(tid, cmn.GCO.Get(), nil, vmd, nil, false); err != nil {
		t.Fatal(err)
	}
	avail, disabled := fs.Get()
	saved := disabled[path]
	if len(avail) != 0 || saved == nil || saved.FS != live.FS {
		t.Fatalf("missing disabled mountpath or filesystem metadata: %v", saved)
	}
	if _, err := fs.Enable(path); err == nil {
		t.Fatal("enabled an absent mountpath")
	}

	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	enabled, err := fs.Enable(path)
	if err != nil {
		t.Fatal(err)
	}
	avail, disabled = fs.Get()
	if enabled == nil || avail[path] != enabled || len(disabled) != 0 ||
		enabled.FS != live.FS || enabled.PathDigest != saved.PathDigest {
		t.Fatalf("mountpath was not re-enabled correctly: %v", enabled)
	}

	// a replacement filesystem must leave the path disabled
	vmd.Mountpaths[path].FsType = "other-filesystem"
	fs.NewTestMFS(mock.NewIOS())
	if err := initMPI(tid, cmn.GCO.Get(), nil, vmd, nil, false); err != nil {
		t.Fatal(err)
	}
	_, err = fs.Enable(path)
	sie, ok := errors.AsType[*fs.ErrStorageIntegrity](err)
	if !ok || sie.Code != fs.SieFsDiffers {
		t.Fatalf("expected sie#50, got %v", err)
	}
	avail, disabled = fs.Get()
	if len(avail) != 0 || disabled[path] == nil {
		t.Fatal("failed re-enable changed mountpath registration")
	}
}

// Init selects the newest VMD and replaces bad and outdated copies:
// - configured mountpaths (a, b, c) store an older copy (a), a bad copy (b), and none (c)
// - the newest copy is on mountpath d that only the older copy lists
// - repeat with fresh mountpaths: the outcome must not depend on map order
func TestInitNewestVMD(t *testing.T) {
	const iters = 8
	tgt := &mock.TargetMock{}
	config := cmn.GCO.BeginUpdate()
	count, paths := config.TestFSP.Count, config.FSP.Paths
	config.TestFSP.Count = 3
	cmn.GCO.CommitUpdate(config)
	t.Cleanup(func() {
		config := cmn.GCO.BeginUpdate()
		config.TestFSP.Count, config.FSP.Paths = count, paths
		cmn.GCO.CommitUpdate(config)
	})

	for range iters {
		var (
			root       = t.TempDir()
			a, b, c, d = mkMpath(t, root, "a"), mkMpath(t, root, "b"), mkMpath(t, root, "c"), mkMpath(t, root, "d")
		)
		saveVMD(t, a, newTestVMD(t, tgt.SID(), 1, a, b, c, d))
		tassert.CheckFatal(t, os.WriteFile(filepath.Join(b, fname.Vmd), []byte("garbage"), 0o644))
		saveVMD(t, d, newTestVMD(t, tgt.SID(), 3, a, b, c, d))

		config := cmn.GCO.BeginUpdate()
		config.FSP.Paths = cos.StrKVs{a: "", b: "", c: ""}
		cmn.GCO.CommitUpdate(config)

		created := Init(tgt, cmn.GCO.Get(), IniCtx{})
		tassert.Fatalf(t, !created, "expected existing volume")

		avail, disabled := fs.Get()
		tassert.Fatalf(t, len(avail) == 4 && len(disabled) == 0, "expected 4 available mountpaths, got %d/%d", len(avail), len(disabled))
		var copies int
		for mpath := range avail {
			v := newVMD(0)
			err := v.load(mpath)
			if cos.IsNotExist(err) {
				continue
			}
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, v.Version == 3, "%s: expected v3, got %s", mpath, v)
			copies++
		}
		tassert.Fatalf(t, copies > 0, "no VMD copies")
	}
}

// a mountpath that becomes available after VMD discovery must not be enabled
func TestInitMPIUncheckedMountpath(t *testing.T) {
	const tid = "target01"
	mpath := mkMpath(t, t.TempDir(), "a")
	vmd := newTestVMD(t, tid, 1, mpath)

	fs.NewTestMFS(mock.NewIOS())
	err := initMPI(tid, cmn.GCO.Get(), nil, vmd, cos.StrSet{}, false)
	tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "became available"), "expected became-available error, got %v", err)
	avail, _ := fs.Get()
	tassert.Fatalf(t, len(avail) == 0, "unchecked mountpath enabled")

	tassert.CheckFatal(t, initMPI(tid, cmn.GCO.Get(), nil, vmd, cos.NewStrSet(mpath), false))
}

func TestVcopiesLoad(t *testing.T) {
	const tid = "target01"
	var (
		root   = t.TempDir()
		mpath  = mkMpath(t, root, "a")
		vfname = filepath.Join(mpath, fname.Vmd)
		vc     = newVcopies(1)
		check  = func(valid, bad bool) {
			t.Helper()
			_, isValid := vc.valid[mpath]
			_, isBad := vc.bad[mpath]
			tassert.Fatalf(t, isValid == valid && isBad == bad, "expected valid=%t bad=%t, got %t %t", valid, bad, isValid, isBad)
		}
	)
	// the latest read replaces the previous one: valid => bad => missing => valid
	saveVMD(t, mpath, newTestVMD(t, tid, 1, mpath))
	vc.load([]string{mpath})
	check(true, false)
	tassert.CheckFatal(t, os.WriteFile(vfname, []byte("garbage"), 0o644))
	vc.load([]string{mpath})
	check(false, true)
	tassert.CheckFatal(t, os.Remove(vfname))
	vc.load([]string{mpath})
	check(false, false)
	saveVMD(t, mpath, newTestVMD(t, tid, 2, mpath))
	vc.load([]string{mpath})
	check(true, false)
}

func TestVMDDiscoveryReread(t *testing.T) {
	const (
		tid     = "target01"
		initial = 7
	)
	for _, tc := range []struct {
		name    string
		version uint64
		wantErr string
	}{
		{name: "missing", wantErr: "VMD v7 disappeared during discovery; retry startup"},
		{name: "older", version: 6, wantErr: "VMD version fell from 7 to 6 during discovery; retry startup"},
		{name: "newer", version: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			a, b := mkMpath(t, root, "a"), mkMpath(t, root, "b")
			saveVMD(t, a, newTestVMD(t, tid, initial, a, b))
			vc := newVcopies(2)
			vc.load([]string{a})
			before, err := vc.newest(tid)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, before != nil && before.Version == initial, "expected VMD v%d, got %v", initial, before)

			tassert.CheckFatal(t, os.Remove(filepath.Join(a, fname.Vmd)))
			if tc.version != 0 {
				saveVMD(t, b, newTestVMD(t, tid, tc.version, b))
			}
			after, err := vc.discover(tid, before, make(cos.StrSet, 2))
			if tc.wantErr != "" {
				tassert.Fatalf(t, after == nil && err != nil && err.Error() == tc.wantErr, "expected %q, got %v, %v", tc.wantErr, after, err)
				return
			}
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, after != nil && after.Version == tc.version, "expected VMD v%d, got %v", tc.version, after)
		})
	}
}

func TestVcopiesStale(t *testing.T) {
	const tid = "target01"
	var (
		root          = t.TempDir()
		newer, old    = mkMpath(t, root, "a"), mkMpath(t, root, "b")
		bad, unused   = mkMpath(t, root, "c"), mkMpath(t, root, "d")
		vmd           = newTestVMD(t, tid, 2, newer, old, bad)
		vc            = newVcopies(4)
		avail         = fs.MPI{newer: nil, old: nil, bad: nil}
		badOnlyUnused = newVcopies(1)
	)
	saveVMD(t, newer, vmd)
	saveVMD(t, old, newTestVMD(t, tid, 1, newer, old, bad))
	tassert.CheckFatal(t, os.WriteFile(filepath.Join(bad, fname.Vmd), []byte("garbage"), 0o644))
	tassert.CheckFatal(t, os.WriteFile(filepath.Join(unused, fname.Vmd), []byte("garbage"), 0o644))
	vc.load([]string{newer, old, bad, unused})

	// the outdated and the bad copy on available mountpaths; not the one on the unused mountpath
	tassert.Fatalf(t, vc.stale(vmd, avail) == 2, "expected 2 stale copies, got %d", vc.stale(vmd, avail))

	badOnlyUnused.load([]string{unused})
	tassert.Fatalf(t, badOnlyUnused.stale(vmd, avail) == 0, "unexpected stale copies")
}

func TestOwnershipError(t *testing.T) {
	const tid = "target01"
	var (
		owner = &fs.ErrStorageIntegrity{Code: fs.SieTargetIDMismatch, Msg: "test"}
		other = &fs.ErrStorageIntegrity{Code: fs.SieNotEqVMD, Msg: "test"}
		pod   = IniCtx{PodRef: "ais/ais-target-1"}
	)
	err := pod.ownershipError(owner, tid)
	sie, ok := errors.AsType[*fs.ErrStorageIntegrity](err)
	tassert.Fatalf(t, ok && sie.Code == fs.SieTargetIDMismatch, "lost the storage integrity error: %v", err)
	tassert.Fatalf(t, strings.Contains(err.Error(), pod.PodRef) && strings.Contains(err.Error(), tid), "missing pod and target ID: %v", err)

	tassert.Fatalf(t, pod.ownershipError(other, tid) == other, "unexpected wrap of %v", other)
	tassert.Fatalf(t, IniCtx{}.ownershipError(owner, tid) == owner, "unexpected wrap without pod")
}

//
// helpers
//

func mkMpath(t *testing.T, root, name string) string {
	mpath := filepath.Join(root, name)
	tassert.CheckFatal(t, os.Mkdir(mpath, 0o755))
	return mpath
}

// VMD that lists the given (existing) mountpaths as enabled, with their actual filesystems
func newTestVMD(t *testing.T, tid string, version uint64, mpaths ...string) *VMD {
	vmd := newVMD(len(mpaths))
	vmd.DaemonID, vmd.Version = tid, version
	for _, mpath := range mpaths {
		mi, err := fs.NewMountpath(mpath, "")
		tassert.CheckFatal(t, err)
		vmd.Mountpaths[mpath] = &fsMpathMD{Path: mpath, Fs: mi.Fs, FsType: mi.FsType, FsID: mi.FsID, Enabled: true}
	}
	return vmd
}

func saveVMD(t *testing.T, mpath string, vmd *VMD) {
	tassert.CheckFatal(t, jsp.SaveMeta(filepath.Join(mpath, fname.Vmd), vmd, nil))
}
