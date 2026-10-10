// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// FQN2Mpath: resolve path to its mountpath

func path2Mpath(path string) (mi *fs.Mountpath, err error) {
	mi, _, err = fs.FQN2Mpath(filepath.Clean(path))
	return mi, err
}

func TestMountpathSearchValid(t *testing.T) {
	fs.NewTestMFS(nil)

	mpath := "/tmp/abc"
	createDirs(t, mpath)
	setAvailableMountPaths(t, mpath)

	mi, err := path2Mpath("/tmp/abc/test")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, mi.Path == mpath, "Actual: [%s]. Expected: [%s]", mi.Path, mpath)
}

func TestMountpathSearchInvalid(t *testing.T) {
	fs.NewTestMFS(nil)

	mpath := "/tmp/abc"
	createDirs(t, mpath)
	setAvailableMountPaths(t, mpath)

	mi, err := path2Mpath("xabc")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "xabc", err)
}

func TestMountpathSearchWhenNoAvailable(t *testing.T) {
	fs.NewTestMFS(nil)
	setAvailableMountPaths(t)

	mi, err := path2Mpath("xabc")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "xabc", err)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	prev := config.TestFSP.Count
	config.TestFSP.Count = 2
	cmn.GCO.CommitUpdate(config)
	t.Cleanup(func() {
		config := cmn.GCO.BeginUpdate()
		config.TestFSP.Count = prev
		cmn.GCO.CommitUpdate(config)
	})

	fs.NewTestMFS(nil)
	createDirs(t, "/tmp/x/z/abc", "/tmp/x/zabc", "/tmp/x/y/abc", "/tmp/x/yabc")
	setAvailableMountPaths(t, "/tmp/x/y", "/tmp/x/z")

	mi, err := path2Mpath("z/abc")
	tassert.Errorf(t, err != nil && mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "z/abc", err)

	mi, err = path2Mpath("/tmp/../tmp/x/z/abc")
	tassert.Errorf(t, err == nil && mi.Path == "/tmp/x/z", "Actual: [%s]. Expected: [%s] (%v)",
		mi, "/tmp/x/z", err)

	mi, err = path2Mpath("/tmp/../tmp/x/y/abc")
	tassert.Errorf(t, err == nil && mi.Path == "/tmp/x/y", "Actual: [%s]. Expected: [%s] (%v)",
		mi, "/tmp/x/y", err)
}

func TestSimilarCases(t *testing.T) {
	fs.NewTestMFS(nil)
	createDirs(t, "/tmp/abc", "/tmp/abx")
	setAvailableMountPaths(t, "/tmp/abc")

	mi, err := path2Mpath("/tmp/abc/q")
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, mi.Path == "/tmp/abc", "Actual: [%s]. Expected: [%s]", mi.Path, "/tmp/abc")

	mi, err = path2Mpath("/abx")
	tassert.Errorf(t, mi == nil, "Expected a nil mountpath info for fqn %q (%v)", "/abx", err)
}

func TestRootMountpath(t *testing.T) {
	fs.NewTestMFS(nil)
	setAvailableMountPaths(t)

	_, err := fs.AddTestMpath("/", "daeID")
	tassert.Errorf(t, err != nil, "Expected failure to add \"/\" mountpath")
}

// replace available mountpaths with the given ones; restore upon cleanup
func setAvailableMountPaths(t *testing.T, paths ...string) {
	t.Helper()
	avail := fs.GetAvail()
	prev := make([]string, 0, len(avail))
	for _, mi := range avail {
		prev = append(prev, mi.Path)
	}
	_setAvail(t, paths)
	t.Cleanup(func() { _setAvail(t, prev) })
}

func _setAvail(t *testing.T, paths []string) {
	for _, mi := range fs.GetAvail() {
		_, err := fs.Remove(mi.Path)
		tassert.CheckError(t, err)
	}
	for _, path := range paths {
		_, err := fs.AddTestMpath(path, "daeID")
		tassert.CheckError(t, err)
	}
}

func createDirs(t *testing.T, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		tassert.CheckFatal(t, cos.CreateDir(dir))
	}
	t.Cleanup(func() {
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	})
}
