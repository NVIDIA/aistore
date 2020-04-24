// Package health provides a basic mountpath health monitor.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package health

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	fsCheckerTmpDir = "/tmp/fshc"
)

func testCheckerMountPaths() *fs.MountedFS {
	cmn.CreateDir(fsCheckerTmpDir)
	cmn.CreateDir(fsCheckerTmpDir + "/1")
	cmn.CreateDir(fsCheckerTmpDir + "/2")
	cmn.CreateDir(fsCheckerTmpDir + "/3")
	cmn.CreateDir(fsCheckerTmpDir + "/4")

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.InitMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("%s/%d", fsCheckerTmpDir, i)
		fs.Mountpaths.Add(name)
	}

	os.RemoveAll(fsCheckerTmpDir + "/3") // one folder is deleted
	fs.Mountpaths.Disable(fsCheckerTmpDir + "/4")
	return fs.Mountpaths
}

func updateTestConfig() {
	config := cmn.GCO.BeginUpdate()
	config.FSHC.Enabled = true
	config.FSHC.ErrorLimit = 2
	cmn.GCO.CommitUpdate(config)
}

type MockFSDispatcher struct {
	faultyPath    string
	faultDetected bool
}

func newMockFSDispatcher(mpathToFail string) *MockFSDispatcher {
	return &MockFSDispatcher{
		faultyPath: mpathToFail,
	}
}

func (d *MockFSDispatcher) DisableMountpath(path, reason string) (disabled bool, err error) {
	d.faultDetected = path == d.faultyPath
	if d.faultDetected {
		return false, fmt.Errorf("fault detected: %s", reason)
	}
	return true, nil
}

func testCheckerCleanup() {
	os.RemoveAll(fsCheckerTmpDir)
}

func TestFSChecker(t *testing.T) {
	mm := memsys.DefaultPageMM()
	defer mm.Terminate()

	updateTestConfig()

	var (
		failedMpath = fsCheckerTmpDir + "/3"
		dispatcher  = newMockFSDispatcher(failedMpath)
		fshc        = NewFSHC(dispatcher, testCheckerMountPaths(), mm, fs.CSM)
	)

	// initial state = 2 available FSes - must pass
	availablePaths, disabledPaths := fshc.mountpaths.Get()
	if len(availablePaths) != 3 || len(disabledPaths) != 1 {
		t.Errorf("Invalid number of mountpaths at start: %v - %v",
			availablePaths, disabledPaths)
	}

	// inaccessible mountpath
	_, _, exists := fshc.testMountpath(
		fsCheckerTmpDir+"/3/testfile", fsCheckerTmpDir+"/3", 4, 1024)
	if exists {
		t.Error("Testing non-existing mountpath must fail")
	}

	// failed mountpath must be disabled
	fshc.runMpathTest(failedMpath, failedMpath+"/dir/testfile")

	if !dispatcher.faultDetected {
		t.Errorf("Faulty mountpath %s was not detected", failedMpath)
	}

	// decision making function
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

	testCheckerCleanup()
}
