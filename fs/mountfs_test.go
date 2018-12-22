/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package fs

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/NVIDIA/dfcpub/cmn"
)

func TestAddNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/nonexistingpath")
	if err == nil {
		t.Error("adding non-existing mountpath succeded")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestAddInvalidMountpaths(t *testing.T) {
	mfs := NewMountedFS()
	mpaths := []string{
		"/local",
		"/cloud",
		"/tmp/local/abcd",
		"/tmp/cloud/abcd",
		"/tmp/abcd/local",
		"/tmp/abcd/cloud",
	}
	// Note: There is no need to create the directories for these mountpaths because the
	// check for os.Stat(mpath) happens after we check if the have '/local' or
	// '/cloud' in their path
	for _, mpath := range mpaths {
		err := mfs.Add(mpath)
		if err == nil {
			t.Errorf("adding invalid mountpath: %q succeeded", mpath)
		}
	}
	assertMountpathCount(t, mfs, 0, 0)
}

func TestAddValidMountpaths(t *testing.T) {
	mfs := NewMountedFS()
	mfs.DisableFsIDCheck()
	mpaths := []string{"/tmp/clouder", "/tmp/locals", "/tmp/locals/err"}

	for _, mpath := range mpaths {
		if _, err := os.Stat(mpath); os.IsNotExist(err) {
			cmn.CreateDir(mpath)
			defer os.RemoveAll(mpath)
		}

		if err := mfs.Add(mpath); err != nil {
			t.Errorf("adding valid mountpath %q failed", mpath)
		}

	}
	assertMountpathCount(t, mfs, 3, 0)

	for _, mpath := range mpaths {
		if err := mfs.Remove(mpath); err != nil {
			t.Errorf("removing valid mountpath %q failed", mpath)
		}
	}
	assertMountpathCount(t, mfs, 0, 0)
}

func TestAddExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddAlreadyAddedMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 1, 0)

	err = mfs.Add("/tmp")
	if err == nil {
		t.Error("adding already added mountpath succeded")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestRemoveNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Remove("/nonexistingpath")
	if err == nil {
		t.Error("removing non-existing mountpath succeded")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestRemoveExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.Remove("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestRemoveDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	mfs.Disable("/tmp")
	assertMountpathCount(t, mfs, 0, 1)

	err = mfs.Remove("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestDisableNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	disabled, exists := mfs.Disable("/tmp")
	if disabled || exists {
		t.Error("disabling was successful or mountpath exists")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestDisableExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.Disable("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)
}

func TestDisableAlreadyDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.Disable("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	disabled, exists = mfs.Disable("/tmp")
	if disabled || !exists {
		t.Error("disabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)
}

func TestEnableNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	enabled, exists := mfs.Enable("/tmp")
	if enabled || exists {
		t.Error("enabling was successful or mountpath exists")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestEnableExistingButNotDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	enabled, exists := mfs.Enable("/tmp")
	if enabled || !exists {
		t.Error("enabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestEnableExistingAndDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.Disable("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	enabled, exists := mfs.Enable("/tmp")
	if !enabled || !exists {
		t.Error("enabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestEnableAlreadyEnabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.Disable("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)

	enabled, exists := mfs.Enable("/tmp")
	if !enabled || !exists {
		t.Error("enabling was not successful or mountpath does not exists")
	}

	enabled, exists = mfs.Enable("/tmp")
	if enabled || !exists {
		t.Error("enabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddMultipleMountpathsWithSameFSID(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.Add("/")
	if err == nil {
		t.Error("adding path with same FSID was successful")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddAndDisableMultipleMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.Add("/dev/null") // /dev/null has different fsid
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 2, 0)

	disabled, exists := mfs.Disable("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 1)
}

func TestStoreLoadIostat(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.Add("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}
	availableMountpaths, _ := mfs.Get()
	mi, ok := availableMountpaths["/tmp"]
	if !ok {
		t.Fatal("Expecting to get /tmp")
	}
	mi.SetIOstats(1, StatDiskUtil, 0.7)
	mi.SetIOstats(1, StatQueueLen, 1.3)
	mi.SetIOstats(2, StatDiskUtil, 1.4)
	mi.SetIOstats(2, StatQueueLen, 2.6)

	//
	// test various min/max, previous/current transitions
	//
	prev, curr := mi.GetIOstats(StatDiskUtil)
	if prev.Min != 0.7 || curr.Min != 1.4 {
		t.Errorf("Wrong util stats [%s:%s], expecting (0.7, 1.4)", prev, curr)
	}
	if prev.Max != 0.7 || curr.Max != 1.4 {
		t.Errorf("Wrong util stats [%s:%s], expecting (0.7, 1.4)", prev, curr)
	}
	mi.SetIOstats(2, StatDiskUtil, math.E)

	prev, curr = mi.GetIOstats(StatDiskUtil)
	if prev.Min != 0.7 || prev.Max != 0.7 {
		t.Errorf("Wrong prev stats %s, expecting (0.7, 0.7)", prev)
	}
	if curr.Min != 1.4 || curr.Max != math.E {
		t.Errorf("Wrong curr stats %s, expecting (0.7, 0.7)", curr)
	}
	mi.SetIOstats(2, StatQueueLen, math.Pi/3)
	prev, curr = mi.GetIOstats(StatQueueLen)
	if prev.Min != 1.3 || curr.Min != math.Pi/3 {
		t.Errorf("Wrong stats [%s:%s], expecting (min=1.3, min=%f)", prev, curr, math.Pi/3)
	}
	mi.SetIOstats(3, StatQueueLen, math.Pi)
	prev, curr = mi.GetIOstats(StatQueueLen)
	if prev.Min != math.Pi/3 || prev.Max != 2.6 || curr.Max != math.Pi {
		t.Errorf("Wrong stats [%s:%s], expecting (%f, %f)", prev, curr, math.Pi/3, math.Pi)
	}
	fmt.Println("Success:", mi)
}

func assertMountpathCount(t *testing.T, mfs *MountedFS, availableCount, disabledCount int) {
	availableMountpaths, disabledMountpaths := mfs.Get()
	if len(availableMountpaths) != availableCount ||
		len(disabledMountpaths) != disabledCount {
		t.Errorf(
			"wrong mountpaths: %d/%d, %d/%d",
			len(availableMountpaths), availableCount,
			len(disabledMountpaths), disabledCount,
		)
	}
}
