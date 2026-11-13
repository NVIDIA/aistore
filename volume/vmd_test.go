// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package volume_test

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn/fname"

	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/volume"
)

func TestVMD(t *testing.T) {
	const (
		mpathsCnt = 10
		daemonID  = "testDaemonID"
	)
	mpaths := tools.PrepareMountPaths(t, mpathsCnt)
	defer tools.RemoveMpaths(t, mpaths)

	t.Run("CreateNewVMD", func(t *testing.T) { testVMDCreate(t, mpaths, daemonID) })
	t.Run("VMDPersist", func(t *testing.T) { testVMDPersist(t, daemonID) })
}

func testVMDCreate(t *testing.T, mpaths fs.MPI, daemonID string) {
	var (
		vmd, err  = volume.NewFromMPI(daemonID)
		mpathsCnt = len(mpaths)
	)

	tassert.CheckFatal(t, err)
	tassert.Errorf(t, vmd.DaemonID == daemonID, "incorrect daemonID, expected %q, got %q", daemonID, vmd.DaemonID)
	tassert.Errorf(t, len(vmd.Mountpaths) == mpathsCnt, "expected %d mpaths, got %d", mpathsCnt, len(vmd.Mountpaths))

	for _, dev := range vmd.Mountpaths {
		_, ok := mpaths[dev.Path]
		tassert.Errorf(t, ok, "vmd has unknown %q mountpath", dev.Path)
	}
}

func testVMDPersist(t *testing.T, daemonID string) {
	vmd, err := volume.NewFromMPI(daemonID)
	tassert.CheckFatal(t, err)

	newVMD, err := volume.LoadVMDTest("")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, newVMD.DaemonID == vmd.DaemonID,
		"expected VMDs to have same daemon ID. got: %s vs %s", newVMD.DaemonID, vmd.DaemonID)
	tassert.Errorf(t, reflect.DeepEqual(newVMD.Mountpaths, vmd.Mountpaths),
		"expected VMDs to be equal. got: %+v vs %+v", newVMD, vmd)
}

// the newest VMD wins regardless of loading order and bad copies
func TestVMDNewest(t *testing.T) {
	const (
		tid   = "GBLgBUup"
		other = "FfIXNSzh"
		iters = 100
	)
	dir := func(tid string, ver uint64, mpaths ...string) string {
		d := t.TempDir()
		tassert.CheckFatal(t, volume.SaveVMDTest(d, tid, ver, mpaths...))
		return d
	}
	garbage := func() string {
		d := t.TempDir()
		tassert.CheckFatal(t, os.WriteFile(filepath.Join(d, fname.Vmd), []byte("garbage"), 0o644))
		return d
	}
	load := func(mpaths ...string) (*volume.VMD, error) {
		rand.Shuffle(len(mpaths), func(i, j int) { mpaths[i], mpaths[j] = mpaths[j], mpaths[i] })
		return volume.LoadVMDTest(tid, mpaths...)
	}
	var (
		v5x   = dir(tid, 5, "/x")
		v5y   = dir(tid, 5, "/y") // differs from v5x below the newest version
		v7    = dir(tid, 7, "/x", "/y")
		v7too = dir(tid, 7, "/x", "/y")
		bad   = garbage()
		empty = t.TempDir()
	)

	t.Run("newest", func(t *testing.T) {
		for range iters {
			vmd, err := load(v5x, v5y, bad, v7, v7too, empty)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, vmd != nil && vmd.Version == 7, "expected v7, got %v", vmd)
		}
	})
	t.Run("conflict", func(t *testing.T) {
		v7x := dir(tid, 7, "/x")
		for range iters {
			_, err := load(v5x, bad, v7, v7x)
			checkSie(t, err, fs.SieNotEqVMD)
		}
	})
	t.Run("other-target", func(t *testing.T) {
		v3 := dir(other, 3, "/x")
		for range iters {
			_, err := load(v5x, v7, v3)
			checkSie(t, err, fs.SieTargetIDMismatch)
		}
	})
	t.Run("all-bad", func(t *testing.T) {
		_, err := load(bad, garbage(), empty)
		checkSie(t, err, fs.SieMetaCorrupted)
	})
	t.Run("none", func(t *testing.T) {
		vmd, err := load(empty, t.TempDir())
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, vmd == nil, "expected no VMD, got %v", vmd)
	})
}

func checkSie(t *testing.T, err error, code int) {
	t.Helper()
	sie, ok := errors.AsType[*fs.ErrStorageIntegrity](err)
	tassert.Fatalf(t, ok, "expected storage integrity error, got %v", err)
	tassert.Fatalf(t, sie.Code == code, "expected sie#%d, got %v", code, err)
}
