/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
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
	ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
	fileSystem := fqn2fsAtStartup(tempRoot)
	mp := &mountPath{Path: tempRoot, FileSystem: fileSystem}
	ctx.mountpaths.Available[mp.Path] = mp
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice

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
	fileSystem := fqn2fsAtStartup(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks := fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/tmp"
	fileSystem = fqn2fsAtStartup(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "/home"
	fileSystem = fqn2fsAtStartup(path)
	if fileSystem == "" {
		t.Errorf("Invalid FS for path: [%s]", path)
	}
	disks = fs2disks(fileSystem)
	if len(disks) == 0 {
		t.Errorf("Invalid FS disks: [%s]", fileSystem)
	}

	path = "asdasd"
	fileSystem = fqn2fsAtStartup(path)
	if fileSystem != "" {
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
	ctx.mountpaths.Available = oldMPs
}

func TestSearchInvalidMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("/")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
}

func TestSearchWithNoMountPath(t *testing.T) {
	oldMPs := setAvailableMountPaths("")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice
}

func TestSearchWithASuffixToAnotherValue(t *testing.T) {
	oldMPs := setAvailableMountPaths("/", "/x")
	longestPrefix := fqn2mountPath("xabc")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	longestPrefix = fqn2mountPath("/xabc")
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	longestPrefix = fqn2mountPath("/x/abc")
	testAssert(t, longestPrefix == "/x", "Actual: [%s]. Expected: [%s]", longestPrefix, "/x")
	ctx.mountpaths.Available = oldMPs
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice
}

func TestSimilarCases(t *testing.T) {
	oldMPs := setAvailableMountPaths("/abc")
	longestPrefix := fqn2mountPath("/abc")
	testAssert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = fqn2mountPath("/abc/")
	testAssert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = fqn2mountPath("/abx")
	testAssert(t, longestPrefix == "", "Actual: [%s]. Expected: [%s]", longestPrefix, "")
	ctx.mountpaths.Available = oldMPs
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice
}

func TestSimilarCasesWithRoot(t *testing.T) {
	oldMPs := setAvailableMountPaths("/abc", "/")
	longestPrefix := fqn2mountPath("/abc")
	testAssert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = fqn2mountPath("/abc/")
	testAssert(t, longestPrefix == "/abc", "Actual: [%s]. Expected: [%s]", longestPrefix, "/abc")
	longestPrefix = fqn2mountPath("/abx")
	testAssert(t, longestPrefix == "/", "Actual: [%s]. Expected: [%s]", longestPrefix, "/")
	ctx.mountpaths.Available = oldMPs
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice
}

func setAvailableMountPaths(paths ...string) map[string]*mountPath {
	oldAvailableMPs := ctx.mountpaths.Available
	ctx.mountpaths.Available = make(map[string]*mountPath, len(ctx.config.FSpaths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		fileSystem := fqn2fsAtStartup(path)
		mp := &mountPath{Path: path, FileSystem: fileSystem}
		ctx.mountpaths.Available[mp.Path] = mp
	}
	ctx.mountpaths.Lock()
	ctx.mountpaths.cloneAndUnlock() // available mpaths => ro slice
	return oldAvailableMPs
}

func testAssert(t *testing.T, condition bool, msg string, args ...interface{}) {
	if !condition {
		t.Errorf(fmt.Sprintf(msg, args...))
	}
}
