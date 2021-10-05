// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"reflect"
	"testing"

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
		vmd, err  = fs.CreateVMD(daemonID)
		mpathsCnt = len(mpaths)
	)

	tassert.Errorf(t, err == nil, "expected vmd to be created without error")
	tassert.Errorf(t, vmd.DaemonID == daemonID, "incorrect daemonID, expected %q, got %q", daemonID, vmd.DaemonID)
	tassert.Errorf(t, len(vmd.Mountpaths) == mpathsCnt, "expected %d mpaths, got %d", mpathsCnt, len(vmd.Mountpaths))

	for _, dev := range vmd.Mountpaths {
		_, ok := mpaths[dev.Path]
		tassert.Errorf(t, ok, "vmd has unknown %q mountpath", dev.Path)
	}
	tassert.Errorf(t, len(mpaths) == len(vmd.Mountpaths),
		"expected mpath set to have size %d, got %d", len(mpaths), len(vmd.Mountpaths))
}

func testVMDPersist(t *testing.T, daemonID string) {
	vmd, err := fs.CreateVMD(daemonID)
	tassert.CheckFatal(t, err)

	newVMD, err := fs.LoadVMDTest()
	tassert.Fatalf(t, err == nil, "expected no error while loading VMD")
	tassert.Errorf(t, newVMD.DaemonID == vmd.DaemonID,
		"expected VMDs to have same daemon ID. got: %s vs %s", newVMD.DaemonID, vmd.DaemonID)
	tassert.Errorf(t, reflect.DeepEqual(newVMD.Mountpaths, vmd.Mountpaths),
		"expected VMDs to be equal. got: %+v vs %+v", newVMD, vmd)
}
