// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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

	cos.CreateDir(fsCheckerTmpDir)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	fs.TestDisableValidation()
	for i := 1; i <= 4; i++ {
		mpath := fmt.Sprintf("%s/%d", fsCheckerTmpDir, i)

		err := cos.CreateDir(mpath)
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

func (d *MockFSDispatcher) DisableMpath(mpath, reason string) (err error) {
	d.faultDetected = cos.StringInSlice(mpath, d.faultyPaths)
	if d.faultDetected {
		err = fmt.Errorf("fault detected: %s", reason)
	}
	return
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
	)
	_, _, exists := testMountpath(filePath, failedMpath, 4, cos.KiB)
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
			res, err := isTestPassed("/tmp", tst.readErrs, tst.writeErrs, tst.avail)
			if res != tst.result {
				t.Errorf("%s failed: expected %v, got %v (%v)", tst.title, tst.result, res, err)
			}
		})
	}
}

func TestFSCheckerTryReadFile(t *testing.T) {
	setupTests(t)

	var (
		mpath    = fsCheckerTmpDir + "/1"
		filePath = mpath + "/smth.txt"
	)

	// Create file with some content inside.
	file, err := cos.CreateFile(filePath)
	tassert.CheckFatal(t, err)
	err = cos.FloodWriter(file, cos.MiB)
	file.Close()
	tassert.CheckFatal(t, err)

	err = tryReadFile(filePath)
	tassert.CheckFatal(t, err)
}

func TestFSCheckerTryWriteFile(t *testing.T) {
	setupTests(t)
	mpath := fsCheckerTmpDir + "/1"
	err := tryWriteFile(mpath, cos.KiB)
	tassert.CheckFatal(t, err)
}
