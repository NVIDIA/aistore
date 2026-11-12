// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package volume_test

import (
	"errors"
	"reflect"
	"testing"

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

	newVMD, err := volume.LoadVMDTest("", "")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, newVMD.DaemonID == vmd.DaemonID,
		"expected VMDs to have same daemon ID. got: %s vs %s", newVMD.DaemonID, vmd.DaemonID)
	tassert.Errorf(t, reflect.DeepEqual(newVMD.Mountpaths, vmd.Mountpaths),
		"expected VMDs to be equal. got: %+v vs %+v", newVMD, vmd)
}

// pod-derived target ID: mountpaths that belong to a different target are rejected
func TestVMDPodID(t *testing.T) {
	const (
		tid   = "daeID" // as stamped by tools.PrepareMountPaths
		other = "GBLgBUup"
		pod   = "ais-target-1"
	)
	mpaths := tools.PrepareMountPaths(t, 4)
	defer tools.RemoveMpaths(t, mpaths)

	// no VMD: same target (e.g., restarted with a new volume)
	vmd, err := volume.LoadVMDTest(tid, pod)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, vmd == nil, "expected no VMD, got %v", vmd)

	// mountpaths stamped by this target, by a different one, and not stamped
	blank := t.TempDir()
	for mpath := range mpaths {
		tassert.CheckFatal(t, volume.CheckPodMpathTest(tid, pod, mpath))
		checkSie(t, volume.CheckPodMpathTest(other, pod, mpath), fs.SiePodIDMismatch)
	}
	tassert.CheckFatal(t, volume.CheckPodMpathTest(other, pod, blank))
	tassert.CheckFatal(t, volume.CheckPodMpathTest(other, pod, blank+"/missing"))

	// VMD of the same target
	_, err = volume.NewFromMPI(tid)
	tassert.CheckFatal(t, err)
	vmd, err = volume.LoadVMDTest(tid, pod)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, vmd != nil && vmd.DaemonID == tid, "expected VMD of %q, got %v", tid, vmd)

	// VMD of a different target, mountpaths stamped by this one
	_, err = volume.NewFromMPI(other)
	tassert.CheckFatal(t, err)
	_, err = volume.LoadVMDTest(tid, pod)
	checkSie(t, err, fs.SiePodIDMismatch)

	// ditto, w/ no pod
	_, err = volume.LoadVMDTest(tid, "")
	checkSie(t, err, fs.SieTargetIDMismatch)
}

func checkSie(t *testing.T, err error, code int) {
	t.Helper()
	var sie *fs.ErrStorageIntegrity
	tassert.Fatalf(t, errors.As(err, &sie), "expected storage integrity error, got %v", err)
	tassert.Fatalf(t, sie.Code == code, "expected sie#%d, got %v", code, err)
}
