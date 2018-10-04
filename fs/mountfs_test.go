/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package fs

import (
	"testing"
)

func TestAddNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/nonexistingpath")
	if err == nil {
		t.Error("adding non-existing mountpath succeded")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestAddInvalidMountpaths(t *testing.T) {
	mfs := NewMountedFS()

	err := mfs.AddMountpath("/local")
	if err == nil {
		t.Error("adding invalid mountpath succeeded")
	}

	err = mfs.AddMountpath("/cloud")
	if err == nil {
		t.Error("adding invalid mountpath succeeded")
	}

	err = mfs.AddMountpath("/tmp/local/abcd")
	if err == nil {
		t.Error("adding invalid mountpath succeeded")
	}

	err = mfs.AddMountpath("/tmp/cloud/abcd")
	if err == nil {
		t.Error("adding invalid mountpath succeeded")
	}
	assertMountpathCount(t, mfs, 0, 0)
}

func TestAddExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddAlreadyAddedMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 1, 0)

	err = mfs.AddMountpath("/tmp")
	if err == nil {
		t.Error("adding already added mountpath succeded")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestRemoveNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.RemoveMountpath("/nonexistingpath")
	if err == nil {
		t.Error("removing non-existing mountpath succeded")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestRemoveExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.RemoveMountpath("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestRemoveDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	mfs.DisableMountpath("/tmp")
	assertMountpathCount(t, mfs, 0, 1)

	err = mfs.RemoveMountpath("/tmp")
	if err != nil {
		t.Error("removing existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestDisableNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	disabled, exists := mfs.DisableMountpath("/tmp")
	if disabled || exists {
		t.Error("disabling was successful or mountpath exists")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestDisableExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.DisableMountpath("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)
}

func TestDisableAlreadyDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.DisableMountpath("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	disabled, exists = mfs.DisableMountpath("/tmp")
	if disabled || !exists {
		t.Error("disabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)
}

func TestEnableNonExistingMountpath(t *testing.T) {
	mfs := NewMountedFS()
	enabled, exists := mfs.EnableMountpath("/tmp")
	if enabled || exists {
		t.Error("enabling was successful or mountpath exists")
	}

	assertMountpathCount(t, mfs, 0, 0)
}

func TestEnableExistingButNotDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	enabled, exists := mfs.EnableMountpath("/tmp")
	if enabled || !exists {
		t.Error("enabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestEnableExistingAndDisabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.DisableMountpath("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	enabled, exists := mfs.EnableMountpath("/tmp")
	if !enabled || !exists {
		t.Error("enabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestEnableAlreadyEnabledMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	disabled, exists := mfs.DisableMountpath("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 0, 1)

	enabled, exists := mfs.EnableMountpath("/tmp")
	if !enabled || !exists {
		t.Error("enabling was not successful or mountpath does not exists")
	}

	enabled, exists = mfs.EnableMountpath("/tmp")
	if enabled || !exists {
		t.Error("enabling was successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddMultipleMountpathsWithSameFSID(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.AddMountpath("/")
	if err == nil {
		t.Error("adding path with same FSID was successful")
	}

	assertMountpathCount(t, mfs, 1, 0)
}

func TestAddAndDisableMultipleMountpath(t *testing.T) {
	mfs := NewMountedFS()
	err := mfs.AddMountpath("/tmp")
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	err = mfs.AddMountpath("/dev/null") // /dev/null has different fsid
	if err != nil {
		t.Error("adding existing mountpath failed")
	}

	assertMountpathCount(t, mfs, 2, 0)

	disabled, exists := mfs.DisableMountpath("/tmp")
	if !disabled || !exists {
		t.Error("disabling was not successful or mountpath does not exists")
	}

	assertMountpathCount(t, mfs, 1, 1)
}

func assertMountpathCount(t *testing.T, mfs *MountedFS, availableCount, disabledCount int) {
	availableMountpaths, disabledMountpaths := mfs.Mountpaths()
	if len(availableMountpaths) != availableCount ||
		len(disabledMountpaths) != disabledCount {
		t.Errorf(
			"wrong mountpaths: %d/%d, %d/%d",
			len(availableMountpaths), availableCount,
			len(disabledMountpaths), disabledCount,
		)
	}
}
