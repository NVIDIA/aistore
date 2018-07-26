/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/fs"
)

func TestGetFSUsedPercentage(t *testing.T) {
	percentage, ok := getFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}

func TestGetFSDiskUtil(t *testing.T) {
	if err := checkIostatVersion(); err != nil {
		t.Skip("iostat version is not okay.")
	}

	tempRoot := "/tmp"
	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, 1)
	ctx.mountpaths.AddMountpath(tempRoot)

	riostat := newIostatRunner()
	go riostat.run()

	time.Sleep(50 * time.Millisecond)
	percentage, ok := riostat.diskUtilFromFQN(tempRoot + "/test")
	if !ok {
		t.Error("Unable to retrieve disk utilization for File System!")
	}
	if percentage < 0 && percentage > 100 {
		t.Errorf("Invalid FS disk utilization percentage [%f].", percentage)
	}
	glog.Infof("Disk utilization fetched. Value [%f]", percentage)
	riostat.stop(fmt.Errorf("test"))
}

func TestGetDiskFromFileSystem(t *testing.T) {
	path := "/"
	fileSystem, err := fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks := fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/tmp"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/home"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "asdasd"
	fileSystem, err = fs.Fqn2fsAtStartup(path)
	if err != nil {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(path)
	if len(disks) != 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}
}

func TestMultipleMountPathsOnSameDisk(t *testing.T) {
	rawJson := json.RawMessage(
		`{
        "blockdevices": [{
                "name": "xvda",
                "children": [{
                    "name": "xvda1"
                }]
            },
            {
                "name": "xvdb",
                "children": [{
                    "name": "md0"
                }]
            }, {
                "name": "xvdd",
                "children": [{
                    "name": "md0"
                }]
            }
        ]
		}`)
	bytes, err := json.Marshal(&rawJson)
	if err != nil {
		t.Errorf("Unable to marshal input json. Error: [%v]", err)
	}
	disks := lsblkOutput2disks(bytes, "md0")
	if len(disks) != 2 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdb"]; !ok {
		t.Errorf("Expected disk [xvdb] not returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdd"]; !ok {
		t.Errorf("Expected disk [xvdd] not returned. Disks: [%v]", disks)
	}
	disks = lsblkOutput2disks(bytes, "xvda1")
	if len(disks) != 1 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvda"]; !ok {
		t.Errorf("Expected disk [xvda] not returned. Disks: [%v]", disks)
	}
}

func TestGetMaxUtil(t *testing.T) {
	riostat := newIostatRunner()
	riostat.Disk = make(map[string]simplekvs, 2)
	disks := make(StringSet)
	disk1 := "disk1"
	disks[disk1] = struct{}{}
	riostat.Disk[disk1] = make(simplekvs, 1)
	riostat.Disk[disk1]["%util"] = "23.2"
	util := maxUtilDisks(riostat.Disk, disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
	disk2 := "disk2"
	riostat.Disk[disk2] = make(simplekvs, 1)
	riostat.Disk[disk2]["%util"] = "25.9"
	disks[disk2] = struct{}{}
	util = maxUtilDisks(riostat.Disk, disks)
	if math.Abs(25.9-util) > 0.0001 {
		t.Errorf("Expected: 25.9. Actual: %f", util)
	}
	delete(disks, disk2)
	util = maxUtilDisks(riostat.Disk, disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
}

func TestGetFSDiskUtilizationInvalid(t *testing.T) {
	riostat := newIostatRunner()
	_, ok := riostat.diskUtilFromFQN("test")
	if ok {
		t.Errorf("Expected to fail since no file system.")
	}
	_, ok = riostat.diskUtilFromFQN("/tmp")
	if ok {
		t.Errorf("Expected to fail since no file system to disk mapping.")
	}
}

func TestSearchValidMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("/")
	longestPrefix := fqn2mountPath("/abc")
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchInvalidMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("/")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithNoMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	setAvailableMountPaths(oldMPs...)
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	dirs := []string{"/tmp/x", "/tmp/xabc", "/tmp/x/abc"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp", "/tmp/x")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	longestPrefix = fqn2mountPath("/tmp/xabc")
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")
	longestPrefix = fqn2mountPath("/tmp/x/abc")
	testAssert(t, longestPrefix == "/tmp/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/x")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCases(t *testing.T) {
	dirs := []string{"/tmp/abc", "/tmp/abx"}
	createDirs(dirs...)
	defer removeDirs(dirs...)

	oldMPs := setAvailableMountPaths("/tmp/abc")
	longestPrefix := fqn2mountPath("/tmp/abc")
	testAssert(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")
	longestPrefix = fqn2mountPath("/tmp/abc/")
	testAssert(t, longestPrefix == "/tmp/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp/abc")
	longestPrefix = fqn2mountPath("/abx")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	setAvailableMountPaths(oldMPs...)
}

func TestSimilarCasesWithRoot(t *testing.T) {
	oldMPs := setAvailableMountPaths("/tmp", "/")
	longestPrefix := fqn2mountPath("/tmp")
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")
	longestPrefix = fqn2mountPath("/tmp/")
	testAssert(t, longestPrefix == "/tmp", "Actual: [%s]. Expected: [%s]", longestPrefix, "/tmp")
	longestPrefix = fqn2mountPath("/abx")
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	setAvailableMountPaths(oldMPs...)
}

func setAvailableMountPaths(paths ...string) []string {
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	oldPaths := make([]string, 0, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		oldPaths = append(oldPaths, mpathInfo.Path)
	}

	for _, mpathInfo := range availablePaths {
		ctx.mountpaths.RemoveMountpath(mpathInfo.Path)
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		ctx.mountpaths.AddMountpath(path)
	}

	return oldPaths
}

func testAssert(t *testing.T, condition bool, msg string, args ...interface{}) {
	if !condition {
		t.Errorf(fmt.Sprintf(msg, args...))
	}
}

func createDirs(dirs ...string) error {
	for _, dir := range dirs {
		err := CreateDir(dir)
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
