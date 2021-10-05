// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

func TestMountpathAddNonExisting(t *testing.T) {
	initFS()

	_, err := fs.Add("/nonexistingpath", "")
	tassert.Errorf(t, err != nil, "adding non-existing mountpath succeeded")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathAddExisting(t *testing.T) {
	initFS()

	tutils.AddMpath(t, "/tmp/abc")
	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathAddValid(t *testing.T) {
	initFS()

	mpaths := []string{"/tmp/clouder", "/tmp/locals", "/tmp/locals/err"}
	for _, mpath := range mpaths {
		tutils.AddMpath(t, mpath)
	}
	tutils.AssertMountpathCount(t, 3, 0)

	for _, mpath := range mpaths {
		removedMP, err := fs.Remove(mpath)
		tassert.Errorf(t, err == nil, "removing valid mountpath %q failed, err: %v", mpath, err)
		tassert.Errorf(t, removedMP != nil, "expected remove to return removed mountpath")
	}
	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathAddIncorrect(t *testing.T) {
	initFS()

	_, err := fs.Add("tmp/not/absolute/path", "")
	tassert.Errorf(t, err != nil, "expected adding incorrect mountpath to fail")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathAddAlreadyAdded(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)
	tutils.AssertMountpathCount(t, 1, 0)

	_, err := fs.Add(mpath, "daeID")
	tassert.Errorf(t, err != nil, "adding already added mountpath succeeded")

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathRemoveNonExisting(t *testing.T) {
	initFS()

	removedMP, err := fs.Remove("/nonexistingpath")
	tassert.Errorf(t, err != nil, "removing non-existing mountpath succeeded")
	tassert.Errorf(t, removedMP == nil, "expected no mountpath removed")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathRemoveExisting(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	removedMP, err := fs.Remove(mpath)
	tassert.CheckError(t, err)
	tassert.Errorf(t, removedMP != nil, "expected remove to return removed mountpath")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathRemoveDisabled(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	_, err := fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tutils.AssertMountpathCount(t, 0, 1)

	removedMP, err := fs.Remove(mpath)
	tassert.CheckError(t, err)
	tassert.Errorf(t, removedMP != nil, "expected remove to return removed mountpath")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathDisableNonExisting(t *testing.T) {
	initFS()

	_, err := fs.Disable("/tmp")
	tassert.Errorf(t, err != nil, "disabling non existing mountpath should not be successful")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathDisableExisting(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	disabledMP, err := fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP != nil, "disabling was not successful")

	tutils.AssertMountpathCount(t, 0, 1)
}

func TestMountpathDisableAlreadyDisabled(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	disabledMP, err := fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP != nil, "disabling was not successful")

	disabledMP, err = fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP == nil, "already disabled mountpath should not be disabled again")

	tutils.AssertMountpathCount(t, 0, 1)
}

func TestMountpathEnableNonExisting(t *testing.T) {
	fs.New()
	_, err := fs.Enable("/tmp")
	tassert.Errorf(t, err != nil, "enabling nonexisting mountpath should end with error")

	tutils.AssertMountpathCount(t, 0, 0)
}

func TestMountpathEnableExistingButNotDisabled(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)
	enabledMP, err := fs.Enable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabledMP == nil, "already enabled mountpath should not be enabled again")

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathEnableExistingAndDisabled(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	disabledMP, err := fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP != nil, "disabling was not successful")

	enabled, err := fs.Enable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabled != nil, "enabling was not successful")

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathEnableAlreadyEnabled(t *testing.T) {
	initFS()

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	disabledMP, err := fs.Disable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP != nil, "disabling was not successful")

	tutils.AssertMountpathCount(t, 0, 1)

	enabledMP, err := fs.Enable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabledMP != nil, "enabling was not successful")

	enabledMP, err = fs.Enable(mpath)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, enabledMP == nil, "enabling already enabled mountpath should not be successful")

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathsAddMultipleWithSameFSID(t *testing.T) {
	fs.New(ios.NewIOStaterMock())

	mpath := "/tmp/abc"
	tutils.AddMpath(t, mpath)

	_, err := fs.Add("/", "")
	tassert.Errorf(t, err != nil, "expected adding path with same FSID to be unsuccessful")

	tutils.AssertMountpathCount(t, 1, 0)
}

func TestMountpathAddAndDisableMultiple(t *testing.T) {
	initFS()

	mp1, mp2 := "/tmp/mp1", "/tmp/mp2"
	tutils.AddMpath(t, mp1)
	tutils.AddMpath(t, mp2)

	tutils.AssertMountpathCount(t, 2, 0)

	disabledMP, err := fs.Disable(mp1)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, disabledMP != nil, "disabling was not successful")
	tutils.AssertMountpathCount(t, 1, 1)
}

func TestMoveToTrash(t *testing.T) {
	initFS()

	mpath := t.TempDir()
	tutils.AddMpath(t, mpath)

	mpaths, _ := fs.Get()
	mi := mpaths[mpath]

	// Initially trash directory should not exist.
	tutils.CheckPathNotExists(t, mi.MakePathTrash())

	// Removing path that don't exist is still good.
	err := mi.MoveToTrash("/path/to/wonderland")
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

func TestMoveMarkers(t *testing.T) {
	tests := []struct {
		name string
		f    func(string, ...func()) (*fs.MountpathInfo, error)
	}{
		{name: "remove", f: fs.Remove},
		{name: "disable", f: fs.Disable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initFS()

			mpath := createMountpath(t)

			fatalErr, writeErr := fs.PersistMarker(cmn.RebalanceMarker)
			tassert.CheckFatal(t, fatalErr)
			tassert.CheckFatal(t, writeErr)

			createMountpath(t)

			exists := fs.MarkerExists(cmn.RebalanceMarker)
			tassert.Fatalf(t, exists, "marker does not exist")

			_, err := test.f(mpath.Path)
			tassert.CheckFatal(t, err)

			exists = fs.MarkerExists(cmn.RebalanceMarker)
			tassert.Fatalf(t, exists, "marker does not exist")
		})
	}
}

func initFS() {
	fs.New(ios.NewIOStaterMock())
	fs.DisableFsIDCheck()
}

func createMountpath(t *testing.T) *fs.MountpathInfo {
	mpathDir := t.TempDir()
	tutils.AddMpath(t, mpathDir)
	mpaths, _ := fs.Get()
	return mpaths[mpathDir]
}

func BenchmarkMakePathFQN(b *testing.B) {
	var (
		bck = cmn.Bck{
			Name:     "bck",
			Provider: cmn.ProviderAzure,
			Ns:       cmn.Ns{Name: "name", UUID: "uuid"},
		}
		mi      = fs.MountpathInfo{Path: cos.RandString(200)}
		objName = cos.RandString(15)
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := mi.MakePathFQN(bck, fs.ObjectType, objName)
		cos.Assert(len(s) > 0)
	}
}
