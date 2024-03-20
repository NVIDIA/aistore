// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// test file for ios/dutils_linux.go
// placed here because it requires fs to set up the testing environment

func TestMountpathSearchValid(t *testing.T) {
	fs.TestNew(nil)

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(t, mpath)
	mi, err := fs.Path2Mpath("/tmp/abc/test")
	tassert.Errorf(t, err == nil && mi.Path == mpath, "Actual: [%s]. Expected: [%s]", mi.Path, mpath)
	setAvailableMountPaths(t, oldMPs...)
}

func TestMountpathSearchInvalid(t *testing.T) {
	fs.TestNew(nil)

	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(t, mpath)
	mi, err := fs.Path2Mpath("xabc")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "xabc", err)
	setAvailableMountPaths(t, oldMPs...)
}

func TestMountpathSearchWhenNoAvailable(t *testing.T) {
	fs.TestNew(nil)
	oldMPs := setAvailableMountPaths(t, "")
	mi, err := fs.Path2Mpath("xabc")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "xabc", err)
	setAvailableMountPaths(t, oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 2
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	dirs := []string{"/tmp/x/z/abc", "/tmp/x/zabc", "/tmp/x/y/abc", "/tmp/x/yabc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths(t, "/tmp/x/y", "/tmp/x/z")

	mi, err := fs.Path2Mpath("z/abc")
	tassert.Errorf(t, err != nil && mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "z/abc", err)

	mi, err = fs.Path2Mpath("/tmp/../tmp/x/z/abc")
	tassert.Errorf(t, err == nil && mi.Path == "/tmp/x/z", "Actual: [%s]. Expected: [%s] (%v)",
		mi, "/tmp/x/z", err)

	mi, err = fs.Path2Mpath("/tmp/../tmp/x/y/abc")
	tassert.Errorf(t, err == nil && mi.Path == "/tmp/x/y", "Actual: [%s]. Expected: [%s] (%v)",
		mi, "/tmp/x/y", err)
	setAvailableMountPaths(t, oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	fs.TestNew(nil)
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths(t, "/tmp/abc")

	mi, err := fs.Path2Mpath("/tmp/abc/q")
	mpath := mi.Path
	tassert.Errorf(t, err == nil && mpath == "/tmp/abc", "Actual: [%s]. Expected: [%s] (%v)", mpath, "/tmp/abc", err)

	mi, err = fs.Path2Mpath("/abx")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "/abx", err)
	setAvailableMountPaths(t, oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	fs.TestNew(nil)
	mpath := "/tmp/abc"
	createDirs(mpath)
	defer removeDirs(mpath)

	oldMPs := setAvailableMountPaths(t)
	// root is an invalid mountpath
	_, err := fs.Add("/", "daeID")
	tassert.Errorf(t, err != nil, "Expected failure to add \"/\" mountpath")
	setAvailableMountPaths(t, oldMPs...)
}

func setAvailableMountPaths(t *testing.T, paths ...string) []string {
	avail := fs.GetAvail()
	oldPaths := make([]string, 0, len(avail))
	for _, mi := range avail {
		oldPaths = append(oldPaths, mi.Path)
	}

	for _, mi := range avail {
		_, err := fs.Remove(mi.Path)
		tassert.Errorf(t, err == nil, "%s (%v)", mi, err)
		debug.AssertNoErr(err)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}
		_, err := fs.Add(path, "daeID")
		if err != nil {
			tassert.Errorf(t, err == nil, "%s (%v)", path, err)
		}
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
