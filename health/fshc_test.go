// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/fs"
)

const (
	fsCheckerTmpDir = "/tmp/fshc"
)

func initMountpaths(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll(fsCheckerTmpDir)
	})

	cmn.CreateDir(fsCheckerTmpDir)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.Init()
	fs.DisableFsIDCheck()
	for i := 1; i <= 4; i++ {
		mpath := fmt.Sprintf("%s/%d", fsCheckerTmpDir, i)

		err := cmn.CreateDir(mpath)
		tassert.CheckFatal(t, err)

		_, err = fs.Add(mpath, "id")
		tassert.CheckFatal(t, err)
	}

	os.RemoveAll(fsCheckerTmpDir + "/3") // One directory is deleted.
	fs.Disable(fsCheckerTmpDir + "/4")
}

func updateTestConfig() {
	config := cmn.GCO.BeginUpdate()
	config.FSHC.Enabled = true
	config.FSHC.ErrorLimit = 2
	cmn.GCO.CommitUpdate(config)
}

type MockFSDispatcher struct {
	faultyPaths   []string
	faultDetected bool
}

func newMockFSDispatcher(mpathsToFail ...string) *MockFSDispatcher {
	return &MockFSDispatcher{
		faultyPaths: mpathsToFail,
	}
}

func (d *MockFSDispatcher) DisableMountpath(path, reason string) (disabled bool, err error) {
	d.faultDetected = cmn.StringInSlice(path, d.faultyPaths)
	if d.faultDetected {
		return false, fmt.Errorf("fault detected: %s", reason)
	}
	return true, nil
}

func setupTests(t *testing.T) {
	updateTestConfig()
	initMountpaths(t)
}

func TestFSCheckerInaccessibleMountpath(t *testing.T) {
	setupTests(t)

	var (
		failedMpath = fsCheckerTmpDir + "/3"
		filePath    = failedMpath + "/testfile"

		dispatcher = newMockFSDispatcher(failedMpath)
		fshc       = NewFSHC(dispatcher)
	)

	_, _, exists := fshc.testMountpath(filePath, failedMpath, 4, cmn.KiB)
	tassert.Errorf(t, !exists, "testing non-existing mountpath must fail")
}

func TestFSCheckerFailedMountpath(t *testing.T) {
	setupTests(t)

	var (
		failedMpath = fsCheckerTmpDir + "/3"

		dispatcher = newMockFSDispatcher(failedMpath)
		fshc       = NewFSHC(dispatcher)
	)

	// Failed mountpath must be disabled.
	fshc.runMpathTest(failedMpath, failedMpath+"/dir/testfile")
	tassert.Errorf(t, dispatcher.faultDetected, "faulty mountpath %s was not detected", failedMpath)
}

func TestFSCheckerDecisionFn(t *testing.T) {
	updateTestConfig()

	fshc := NewFSHC(nil)

	// Decision making function.
	type tstInfo struct {
		title               string
		readErrs, writeErrs int
		avail, result       bool
	}
	testList := []tstInfo{
		{"Inaccessible mountpath", 0, 0, false, false},
		{"Healthy mountpath", 0, 0, true, true},
		{"Unstable but OK mountpath", 1, 1, true, true},
		{"Reads failed", 3, 0, true, false},
		{"Writes failed", 1, 3, true, false},
		{"Reads and writes failed", 3, 3, true, false},
	}

	for _, tst := range testList {
		t.Run(tst.title, func(t *testing.T) {
			res, _ := fshc.isTestPassed("/tmp", tst.readErrs, tst.writeErrs, tst.avail)
			if res != tst.result {
				t.Errorf("%s failed. %v expected but %v got", tst.title, tst.result, res)
			}
		})
	}
}

func TestFSCheckerTryReadFile(t *testing.T) {
	setupTests(t)

	var (
		dispatcher = newMockFSDispatcher()
		fshc       = NewFSHC(dispatcher)

		mpath    = fsCheckerTmpDir + "/1"
		filePath = mpath + "/smth.txt"
	)

	// Create file with some content inside.
	file, err := cmn.CreateFile(filePath)
	tassert.CheckFatal(t, err)
	err = cmn.FloodWriter(file, cmn.MiB)
	file.Close()
	tassert.CheckFatal(t, err)

	err = fshc.tryReadFile(filePath)
	tassert.CheckFatal(t, err)
}

func TestFSCheckerTryWriteFile(t *testing.T) {
	setupTests(t)

	var (
		dispatcher = newMockFSDispatcher()
		fshc       = NewFSHC(dispatcher)

		mpath = fsCheckerTmpDir + "/1"
	)

	err := fshc.tryWriteFile(mpath, cmn.KiB)
	tassert.CheckFatal(t, err)
}
