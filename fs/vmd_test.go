// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestVMD(t *testing.T) {
	const (
		mpathsCnt = 10
		daemonID  = "testDaemonID"
	)
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMountPaths(t, mpaths)

	t.Run("CreateVMD", func(t *testing.T) { testVMDCreate(t, mpaths, daemonID) })
	t.Run("VMDPersist", func(t *testing.T) { testVMDPersist(t, daemonID) })
}

func testVMDCreate(t *testing.T, mpaths fs.MPI, daemonID string) {
	var (
		vmd       = fs.CreateVMD(daemonID)
		mpathsCnt = len(mpaths)
	)
	tassert.Errorf(t, vmd != nil, "expected vmd to be created")
	tassert.Errorf(t, vmd.DaemonID == daemonID, "incorrect daemonID, expected %q, got %q", daemonID, vmd.DaemonID)
	tassert.Errorf(t, len(vmd.Devices) == mpathsCnt, "expected %d devices found, got %d", mpathsCnt, len(vmd.Devices))

	devicesSet := cmn.NewStringSet()
	for _, dev := range vmd.Devices {
		devicesSet.Add(dev.MountPath)
		_, ok := mpaths[dev.MountPath]
		tassert.Errorf(t, ok, "vmd has unknown %q mountpath", dev.MountPath)
	}
	tassert.Errorf(t, len(mpaths) == len(vmd.Devices), "expected devices set to have size %d, got %d", len(mpaths), len(vmd.Devices))
}

func testVMDPersist(t *testing.T, daemonID string) {
	vmd := fs.CreateVMD(daemonID)
	tassert.Fatalf(t, vmd != nil, "expected vmd to be created")

	tassert.CheckFatal(t, vmd.Persist())
	err, newVMD := fs.ReadVMD()
	tassert.Fatalf(t, err == nil, "expected no error while loading VMD")
	tassert.Fatalf(t, newVMD != nil, "expected vmd to be not nil")
	// TODO -- FIXME: Use checksum to compare
	tassert.Errorf(t, vmd.DaemonID == newVMD.DaemonID, "expected VMDs to have same daemon ID. got: %s vs %s", vmd.DaemonID, newVMD.DaemonID)
	tassert.Errorf(t, reflect.DeepEqual(vmd.Devices, newVMD.Devices), "expected VMDs to be equal. got: %+v vs %+v", vmd, newVMD)
}
