// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func TestSearchValidMountPath(t *testing.T) {
	InitMountedFS()
	oldMPs := setAvailableMountPaths("/tmp")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchInvalidMountPath(t *testing.T) {
	InitMountedFS()
	oldMPs := setAvailableMountPaths("/tmp")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithNoMountPath(t *testing.T) {
	InitMountedFS()
	oldMPs := setAvailableMountPaths("")
	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	InitMountedFS()
	dirs := []string{"/tmp/x", "/tmp/xabc", "/tmp/x/abc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp", "/tmp/x")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/xabc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/x/abc")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	InitMountedFS()
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/abc")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/tmp/abc/")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Mountpaths.Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "/abx")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	// root is an invalid mountpath
	InitMountedFS()
	oldMPs := setAvailableMountPaths("/tmp", "/")

	mpathInfo, _ := Mountpaths.Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected mpathInfo to be nil when no valid matching mountpath")
	setAvailableMountPaths(oldMPs...)
}

func setAvailableMountPaths(paths ...string) []string {
	Mountpaths.DisableFsIDCheck()

	availablePaths, _ := Mountpaths.Get()
	oldPaths := make([]string, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		oldPaths = append(oldPaths, mpathInfo.Path)
	}

	for _, mpathInfo := range availablePaths {
		Mountpaths.Remove(mpathInfo.Path)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		Mountpaths.Add(path)
	}

	return oldPaths
}

func createDirs(dirs ...string) error {
	for _, dir := range dirs {
		err := cmn.CreateDir(dir)
		if err != nil {
			return err
		}
	}

	return nil
}

func removeDirs(dirs ...string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}
