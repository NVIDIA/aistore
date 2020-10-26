// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestAddNonExistingMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/nonexistingpath")
	if err == nil {
		t.Error("adding non-existing mountpath succeeded")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestAddValidMountpaths(t *testing.T) {
	fs.Init()
	fs.DisableFsIDCheck()
	mpaths := []string{"/tmp/clouder", "/tmp/locals", "/tmp/locals/err"}

	for _, mpath := range mpaths {
		if _, err := os.Stat(mpath); os.IsNotExist(err) {
			cmn.CreateDir(mpath)
			defer os.RemoveAll(mpath)
		}

		if err := fs.Add(mpath); err != nil {
			t.Errorf("adding valid mountpath %q failed", mpath)
		}
	}
	tutils.AssertMountpathCount(t, 3, 0)

	for _, mpath := range mpaths {
		if err := fs.Remove(mpath); err != nil {
			t.Errorf("removing valid mountpath %q failed", mpath)
		}
	}
	tutils.AssertMountpathCount(t, 0, 0)
}

func TestAddExistingMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestAddIncorrectMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("tmp/not/absolute/path")
	if err == nil {
		t.Error("expected adding incorrect mountpath to fail")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestAddAlreadyAddedMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	tutils.AssertMountpathCount(t, 1, 0)

	err = fs.Add("/tmp")
	if err == nil {
		t.Error("adding already added mountpath succeeded")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestRemoveNonExistingMountpath(t *testing.T) {
	fs.Init()
	err := fs.Remove("/nonexistingpath")
	if err == nil {
		t.Error("removing non-existing mountpath succeeded")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestRemoveExistingMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = fs.Remove("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestRemoveDisabledMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	fs.Disable("/tmp")
	tutils.AssertMountpathCount(t, 0, 1)

	err = fs.Remove("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestDisableNonExistingMountpath(t *testing.T) {
	fs.Init()
	_, err := fs.Disable("/tmp")

	if err == nil {
		t.Error("disabling non existing mountpath should not be successful")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestDisableExistingMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, err := fs.Disable("/tmp")
	tassert.CheckFatal(t, err)
	if !disabled {
		t.Error("disabling was not successful")
	}

	tutils.AssertMountpathCount(t, 0, 1)
}

func TestDisableAlreadyDisabledMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, err := fs.Disable("/tmp")
	tassert.CheckFatal(t, err)
	if !disabled {
		t.Error("disabling was not successful")
	}

	disabled, err = fs.Disable("/tmp")
	tassert.CheckFatal(t, err)
	if disabled {
		t.Error("already disabled mountpath should not be disabled again")
	}

	tutils.AssertMountpathCount(t, 0, 1)
}

func TestEnableNonExistingMountpath(t *testing.T) {
	fs.Init()
	_, err := fs.Enable("/tmp")
	if err == nil {
		t.Error("enabling nonexisting mountpath should not end with error")
	}

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestEnableExistingButNotDisabledMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	enabled, err := fs.Enable("/tmp")
	tassert.CheckFatal(t, err)
	if enabled {
		t.Error("already enabled mountpath should not be enabled again")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestEnableExistingAndDisabledMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, err := fs.Disable("/tmp")
	tassert.CheckFatal(t, err)
	if !disabled {
		t.Error("disabling was not successful")
	}

	enabled, err := fs.Enable("/tmp")
	tassert.CheckFatal(t, err)
	if !enabled {
		t.Error("enabling was not successful")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestEnableAlreadyEnabledMountpath(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, err := fs.Disable("/tmp")
	tassert.CheckFatal(t, err)
	if !disabled {
		t.Error("disabling was not successful")
	}

	tutils.AssertMountpathCount(t, 0, 1)

	enabled, err := fs.Enable("/tmp")
	tassert.CheckFatal(t, err)
	if !enabled {
		t.Error("enabling was not successful")
	}

	enabled, err = fs.Enable("/tmp")
	tassert.CheckFatal(t, err)
	if enabled {
		t.Error("enabling already enabled mountpath should not be successful")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestAddMultipleMountpathsWithSameFSID(t *testing.T) {
	fs.Init()
	err := fs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = fs.Add("/")
	if err == nil {
		t.Error("adding path with same FSID was successful")
	}

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestAddAndDisableMultipleMountpath(t *testing.T) {
	fs.Init()
	fs.DisableFsIDCheck()

	mp1, mp2 := "/tmp/mp1", "/tmp/mp2"
	cmn.CreateDir(mp1)
	cmn.CreateDir(mp2)
	defer func() {
		os.Remove(mp2)
		os.Remove(mp1)
	}()

	err := fs.Add(mp1)
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = fs.Add(mp2) // fsid check disabled
	if err != nil {
		t.Fatalf("adding existing mountpath failed %v", err)
	}

	tutils.AssertMountpathCount(t, 2, 0)

	disabled, err := fs.Disable(mp1)
	tassert.CheckFatal(t, err)
	if !disabled {
		t.Error("disabling was not successful")
	}

	tutils.AssertMountpathCount(t, 1, 1)
}

func TestMoveToTrash(t *testing.T) {
	fs.Init()
	mpathDir, err := ioutil.TempDir("", "")
	tassert.CheckFatal(t, err)
	err = fs.Add(mpathDir)
	tassert.CheckFatal(t, err)

	defer os.RemoveAll(mpathDir)

	mpaths, _ := fs.Get()
	mi := mpaths[mpathDir]

	// Initially trash directory should not exist.
	tutils.CheckPathNotExists(t, mi.MakePathTrash())

	// Removing path that don't exist is still good.
	err = mi.MoveToTrash("/path/to/wonderland")
	tassert.CheckFatal(t, err)

	for i := 0; i < 5; i++ {
		topDir, _ := tutils.PrepareDirTree(t, tutils.DirTreeDesc{
			Dirs:  10,
			Files: 10,
			Depth: 2,
			Empty: false,
		})

		tutils.CheckPathExists(t, topDir, true /*dir*/)

		err = mi.MoveToTrash(topDir)
		tassert.CheckFatal(t, err)

		tutils.CheckPathNotExists(t, topDir)
		tutils.CheckPathExists(t, mi.MakePathTrash(), true /*dir*/)
	}
}

func BenchmarkMakePathFQN(b *testing.B) {
	var (
		bck = cmn.Bck{
			Name:     "bck",
			Provider: cmn.ProviderAzure,
			Ns:       cmn.Ns{Name: "name", UUID: "uuid"},
		}
		mi      = fs.MountpathInfo{Path: cmn.RandString(200)}
		objName = cmn.RandString(15)
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := mi.MakePathFQN(bck, fs.ObjectType, objName)
		cmn.Assert(len(s) > 0)
	}
}
