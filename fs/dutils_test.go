// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func TestSearchValidMountPath(t *testing.T) {
	Init()

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(mpath)
	mpathInfo, _ := Path2MpathInfo("/tmp/abc/test")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == mpath, "Actual: [%s]. Expected: [%s]", longestPrefix, mpath)
	setAvailableMountPaths(oldMPs...)
}

func TestSearchInvalidMountPath(t *testing.T) {
	Init()

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(mpath)
	mpathInfo, _ := Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithNoMountPath(t *testing.T) {
	Init()
	oldMPs := setAvailableMountPaths("")
	mpathInfo, _ := Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	Init()
	dirs := []string{"/tmp/x", "/tmp/x/y", "/tmp/x/y/abc", "/tmp/x/yabc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/x", "/tmp/x/y")

	mpathInfo, _ := Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")

	mpathInfo, _ = Path2MpathInfo("/tmp/x/yabc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")

	mpathInfo, _ = Path2MpathInfo("/tmp/x/y/abc")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/x/y", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	Init()
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/abc")

	mpathInfo, _ := Path2MpathInfo("/tmp/abc")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Path2MpathInfo("/tmp/abc/")
	longestPrefix = mpathInfo.Path
	tassert.Errorf(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")

	mpathInfo, _ = Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "/abx")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	// root is an invalid mountpath
	Init()
	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(mpath, "/")
	mpathInfo, _ := Path2MpathInfo("/abx")
	tassert.Errorf(t, mpathInfo == nil, "Expected mpathInfo to be nil when no valid matching mountpath")
	setAvailableMountPaths(oldMPs...)
}

func setAvailableMountPaths(paths ...string) []string {
	DisableFsIDCheck()

	availablePaths, _ := Get()
	oldPaths := make([]string, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		oldPaths = append(oldPaths, mpathInfo.Path)
	}

	for _, mpathInfo := range availablePaths {
		Remove(mpathInfo.Path)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		Add(path, "daeID")
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
