/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"encoding/json"
	"fmt"
	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"math"
	"testing"
	"time"
)

func TestGetFSUsedPercentage(t *testing.T) {
	ioStatRunner := NewIostatRunner()
	percentage, ok := ioStatRunner.getFSUsedPercentage("/")
	if !ok {
		t.Error("Unable to retrieve FS used percentage!")
	}
	if percentage < 0 && percentage > 100 {
		t.Errorf("Invalid FS used percentage [%d].", percentage)
	}
}

func TestGetFSDiskUtil(t *testing.T) {
	if err := CheckIostatVersion(); err != nil {
		t.Skip("iostat version is not okay.")
	}

	tempRoot := "/tmp"
	ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
	fileSystem := getFileSystemFromPath(tempRoot)
	mp := &mountPath{Path: tempRoot, FileSystem: fileSystem}
	ctx.mountpaths.Available[mp.Path] = mp

	ioStatRunner := NewIostatRunner()
	go ioStatRunner.run()

	time.Sleep(50 * time.Millisecond)
	percentage, ok := ioStatRunner.getDiskUtilizationFromPath(tempRoot + "/test")
	if !ok {
		t.Error("Unable to retrieve disk utilization for File System!")
	}
	if percentage < 0 && percentage > 100 {
		t.Errorf("Invalid FS disk utilization percentage [%f].", percentage)
	}
	glog.Infof("Disk utilization fetched. Value [%f]", percentage)
	ioStatRunner.stop(fmt.Errorf("test"))
}

func TestGetDiskFromFileSystem(t *testing.T) {
	path := "/"
	fileSystem := getFileSystemFromPath(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks := getDiskFromFileSystem(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/tmp"
	fileSystem = getFileSystemFromPath(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = getDiskFromFileSystem(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/home"
	fileSystem = getFileSystemFromPath(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = getDiskFromFileSystem(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "asdasd"
	fileSystem = getFileSystemFromPath(path)
	if fileSystem != "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = getDiskFromFileSystem(path)
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
	disks := getDisksFromLsblkOutput(bytes, "md0")
	if len(disks) != 2 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdb"]; !ok {
		t.Errorf("Expected disk [xvdb] not returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvdd"]; !ok {
		t.Errorf("Expected disk [xvdd] not returned. Disks: [%v]", disks)
	}
	disks = getDisksFromLsblkOutput(bytes, "xvda1")
	if len(disks) != 1 {
		t.Errorf("Invalid number of disks returned. Disks: [%v]", disks)
	}
	if _, ok := disks["xvda"]; !ok {
		t.Errorf("Expected disk [xvda] not returned. Disks: [%v]", disks)
	}
}

func TestGetMaxUtil(t *testing.T) {
	ioStatRunner := NewIostatRunner()
	ioStatRunner.Disk = make(map[string]simplekvs, 2)
	disks := make(StringSet)
	disk1 := "disk1"
	disks[disk1] = struct{}{}
	ioStatRunner.Disk[disk1] = make(simplekvs, 1)
	ioStatRunner.Disk[disk1]["%util"] = "23.2"
	util := ioStatRunner.getMaxUtil(disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
	disk2 := "disk2"
	ioStatRunner.Disk[disk2] = make(simplekvs, 1)
	ioStatRunner.Disk[disk2]["%util"] = "25.9"
	disks[disk2] = struct{}{}
	util = ioStatRunner.getMaxUtil(disks)
	if math.Abs(25.9-util) > 0.0001 {
		t.Errorf("Expected: 25.9. Actual: %f", util)
	}
	delete(disks, disk2)
	util = ioStatRunner.getMaxUtil(disks)
	if math.Abs(23.2-util) > 0.0001 {
		t.Errorf("Expected: 23.2. Actual: %f", util)
	}
}

func TestGetFSDiskUtilizationInvalid(t *testing.T) {
	ioStatRunner := NewIostatRunner()
	_, ok := ioStatRunner.getDiskUtilizationFromPath("test")
	if ok {
		t.Errorf("Expected to fail since no file system.")
	}
	_, ok = ioStatRunner.getDiskUtilizationFromPath("/tmp")
	if ok {
		t.Errorf("Expected to fail since no file system to disk mapping.")
	}
}

func TestSearchValidMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("/")
	longestPrefix := getMountPathFromFilePath("/abc")
	Assert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	ctx.mountpaths.Available = oldMPs
}

func TestSearchInvalidMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("/")
	longestPrefix := getMountPathFromFilePath("xabc")
	Assert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
}

func TestSearchWithNoMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("")
	longestPrefix := getMountPathFromFilePath("xabc")
	Assert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	oldMPs := setAvailableMountPaths("/", "/x")
	longestPrefix := getMountPathFromFilePath("xabc")
	Assert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	longestPrefix = getMountPathFromFilePath("/xabc")
	Assert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	longestPrefix = getMountPathFromFilePath("/x/abc")
	Assert(t, longestPrefix == "/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/x")
	ctx.mountpaths.Available = oldMPs
}

func TestRelativePaths(t *testing.T) {
	oldMPs := setAvailableMountPaths("/abc/../", "/abx")
	longestPrefix := getMountPathFromFilePath("/abc")
	Assert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	longestPrefix = getMountPathFromFilePath("/abx/def/../")
	Assert(t, longestPrefix == "/abx", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abx")
	ctx.mountpaths.Available = oldMPs
}

func TestSimilarCases(t *testing.T) {
	oldMPs := setAvailableMountPaths("/abc")
	longestPrefix := getMountPathFromFilePath("/abc")
	Assert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = getMountPathFromFilePath("/abc/")
	Assert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = getMountPathFromFilePath("/abx")
	Assert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
}

func TestSimilarCasesWithRoot(t *testing.T) {
	oldMPs := setAvailableMountPaths("/abc", "/")
	longestPrefix := getMountPathFromFilePath("/abc")
	Assert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = getMountPathFromFilePath("/abc/")
	Assert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = getMountPathFromFilePath("/abx")
	Assert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	ctx.mountpaths.Available = oldMPs
}

func setAvailableMountPaths(paths ...string) map[string]*mountPath {
	oldAvailableMPs := ctx.mountpaths.Available
	ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		fileSystem := getFileSystemFromPath(path)
		mp := &mountPath{Path: path, FileSystem: fileSystem}
		ctx.mountpaths.Available[mp.Path] = mp
	}
	return oldAvailableMPs
}

func Assert(t *testing.T, condition bool, msg string, args ...interface{}) {
	if !condition {
		t.Errorf(fmt.Sprintf(msg, args...))
	}
}
