// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
)

func TestVMD(t *testing.T) {
	const (
		mpathsCnt = 10
		daemonID  = "testDaemonID"
	)
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMountPaths(t, mpaths)

	t.Run("CreateNewVMD", func(t *testing.T) { testVMDCreate(t, mpaths, daemonID) })
	t.Run("VMDPersist", func(t *testing.T) { testVMDPersist(t, daemonID) })
}

func testVMDCreate(t *testing.T, mpaths fs.MPI, daemonID string) {
	var (
		vmd, err  = fs.CreateNewVMD(daemonID)
		mpathsCnt = len(mpaths)
	)

	tassert.Errorf(t, err == nil, "expected vmd to be created without error")
	tassert.Fatalf(t, vmd != nil, "expected vmd to be created")
	tassert.Errorf(t, vmd.DaemonID == daemonID, "incorrect daemonID, expected %q, got %q", daemonID, vmd.DaemonID)
	tassert.Errorf(t, len(vmd.Mountpaths) == mpathsCnt, "expected %d mpaths, got %d", mpathsCnt, len(vmd.Mountpaths))

	for _, dev := range vmd.Mountpaths {
		_, ok := mpaths[dev.Mountpath]
		tassert.Errorf(t, ok, "vmd has unknown %q mountpath", dev.Mountpath)
	}
	tassert.Errorf(t, len(mpaths) == len(vmd.Mountpaths), "expected mpath set to have size %d, got %d", len(mpaths), len(vmd.Mountpaths))
}

func testVMDPersist(t *testing.T, daemonID string) {
	vmd, err := fs.CreateNewVMD(daemonID)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, vmd != nil, "expected vmd to be created")

	available, _ := fs.Get()
	mps := make(cos.StringSet, len(available))
	for _, mp := range available {
		mps.Add(mp.Path)
	}

	newVMD, err := fs.LoadVMD(mps)
	tassert.Fatalf(t, err == nil, "expected no error while loading VMD")
	tassert.Fatalf(t, newVMD != nil, "expected vmd to be not nil")
	tassert.Errorf(t, newVMD.DaemonID == vmd.DaemonID, "expected VMDs to have same daemon ID. got: %s vs %s", newVMD.DaemonID, vmd.DaemonID)
	tassert.Errorf(t, reflect.DeepEqual(newVMD.Mountpaths, vmd.Mountpaths), "expected VMDs to be equal. got: %+v vs %+v", newVMD, vmd)
}
