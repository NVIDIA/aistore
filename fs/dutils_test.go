// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func TestMountpathSearchValid(t *testing.T) {
	New()

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(mpath)
	mpathInfo, _ := Path2MpathInfo("/tmp/abc/test")
	longestPrefix := mpathInfo.Path
	tassert.Errorf(t, longestPrefix == mpath, "Actual: [%s]. Expected: [%s]", longestPrefix, mpath)
	setAvailableMountPaths(oldMPs...)
}

func TestMountpathSearchInvalid(t *testing.T) {
	New()

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(mpath)
	mpathInfo, _ := Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestMountpathSearchWhenNoAvailable(t *testing.T) {
	New()
	oldMPs := setAvailableMountPaths("")
	mpathInfo, _ := Path2MpathInfo("xabc")
	tassert.Errorf(t, mpathInfo == nil, "Expected a nil mountpath info for fqn %q", "xabc")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	New()
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
	New()
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
	New()
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
		_, err := Remove(mpathInfo.Path)
		debug.AssertNoErr(err)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}
		_, err := Add(path, "daeID")
		_ = err
	}

	return oldPaths
}

func createDirs(dirs ...string) {
	for _, dir := range dirs {
		err := cos.CreateDir(dir)
		debug.AssertNoErr(err)
	}
}

func removeDirs(dirs ...string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}
