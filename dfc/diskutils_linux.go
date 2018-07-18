// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"path/filepath"
)

const (
	iostatnumsys     = 6
	iostatnumdsk     = 14
	iostatMinVersion = 11
)

// NewIostatRunner initalizes iostatrunner struct with default values
func NewIostatRunner() *iostatrunner {
	return &iostatrunner{
		chsts:       make(chan struct{}, 1),
		Disk:        make(map[string]simplekvs, 0),
		metricnames: make([]string, 0),
	}
}

type LsBlk struct {
	BlockDevices []BlockDevice `json:"blockdevices"`
}

type BlockDevice struct {
	Name              string             `json:"name"`
	ChildBlockDevices []ChildBlockDevice `json:"children"`
}

type ChildBlockDevice struct {
	Name string `json:"name"`
}

// iostat -cdxtm 10
func (r *iostatrunner) run() (err error) {
	iostatival := strconv.Itoa(int(ctx.config.Periodic.StatsTime / time.Second))
	r.FileSystemToDisksMap = getFileSystemToDiskMap()
	cmd := exec.Command("iostat", "-c", "-d", "-x", "-t", "-m", iostatival)
	stdout, err := cmd.StdoutPipe()
	reader := bufio.NewReader(stdout)
	if err != nil {
		return
	}
	if err = cmd.Start(); err != nil {
		return
	}

	// Assigning started process
	r.process = cmd.Process

	glog.Infof("Starting %s", r.name)
	for {
		b, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		line := string(b)
		fields := strings.Fields(line)
		if len(fields) == iostatnumsys {
			r.Lock()
			r.CPUidle = fields[iostatnumsys-1]
			r.Unlock()
		} else if len(fields) >= iostatnumdsk {
			if strings.HasPrefix(fields[0], "Device") {
				if len(r.metricnames) == 0 {
					r.metricnames = append(r.metricnames, fields[1:]...)
				}
			} else {
				r.Lock()
				device := fields[0]
				var (
					iometrics simplekvs
					ok        bool
				)
				if iometrics, ok = r.Disk[device]; !ok {
					iometrics = make(simplekvs, len(fields)-1) // first time
				}
				for i := 1; i < len(fields); i++ {
					name := r.metricnames[i-1]
					iometrics[name] = fields[i]
				}
				r.Disk[device] = iometrics
				r.Unlock()
			}
		}
		select {
		case <-r.chsts:
			return nil
		default:
		}
	}
	return
}

func (r *iostatrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chsts <- v
	close(r.chsts)

	// Kill process if started
	if r.process != nil {
		if err := r.process.Kill(); err != nil {
			glog.Errorf("Failed to kill iostat, err: %v", err)
		}
	}
}

func (r *iostatrunner) isZeroUtil(dev string) bool {
	iometrics := r.Disk[dev]
	if utilstr, ok := iometrics["%util"]; ok {
		if util, err := strconv.ParseFloat(utilstr, 32); err == nil {
			if util == 0 {
				return true
			}
		}
	}
	return false
}

func (r *iostatrunner) getDiskUtilizationFromPath(path string) (utilization float32, ok bool) {
	fileSystem := getFileSystemUsingMountPath(path)
	if fileSystem == "" {
		return
	}
	return r.getDiskUtilizationFromFileSystem(fileSystem)
}

func (r *iostatrunner) getDiskUtilizationFromFileSystem(fileSystem string) (utilization float32, ok bool) {
	disks, isOk := r.FileSystemToDisksMap[fileSystem]
	if !isOk {
		return
	}
	utilization = float32(r.getMaxUtil(disks))
	if utilization < 0 {
		return
	}
	return utilization, true
}

func (r *iostatrunner) getMaxUtil(disks StringSet) (maxutil float64) {
	maxutil = -1
	if err := CheckIostatVersion(); err != nil {
		return
	}
	r.Lock()
	if len(disks) > 0 {
		for disk := range disks {
			if ioMetrics, ok := r.Disk[disk]; ok {
				if utilStr, ok := ioMetrics["%util"]; ok {
					if util, err := strconv.ParseFloat(utilStr, 32); err == nil {
						if util > maxutil {
							maxutil = util
						}
					}
				}
			}
		}
		r.Unlock()
		return
	}
	for _, iometrics := range r.Disk {
		if utilstr, ok := iometrics["%util"]; ok {
			if util, err := strconv.ParseFloat(utilstr, 32); err == nil {
				if util > maxutil {
					maxutil = util
				}
			}
		}
	}
	r.Unlock()
	return
}

func getFileSystemUsingMountPath(filePath string) (fileSystem string) {
	mountPath := getMountPathFromFilePath(filePath)
	if mountPath == "" {
		return
	}
	return ctx.mountpaths.Available[mountPath].FileSystem
}

func getMountPathFromFilePath(filePath string) string {
	matchingMountPaths := make(StringSet)
	for mountPath := range ctx.mountpaths.Available {
		cleanMountPath := filepath.Clean(mountPath)
		relativePath, err := filepath.Rel(cleanMountPath, filepath.Clean(filePath))
		// If the relative path starts with two dots, it means that the file
		// path is outside the mount path.
		if err != nil || strings.HasPrefix(relativePath, "..") {
			continue
		}
		matchingMountPaths[cleanMountPath] = struct{}{}
	}

	maxLength := -1
	longestPrefixMountPath := ""
	for path := range matchingMountPaths {
		pathLength := len(path)
		assert(pathLength != maxLength, fmt.Sprintf("Path length cannot be similar to an existing length"))
		if pathLength > maxLength {
			maxLength = pathLength
			longestPrefixMountPath = path
		}
	}

	return longestPrefixMountPath
}

func getFileSystemFromPath(fsPath string) (fileSystem string) {
	getFSCommand := fmt.Sprintf("df -P '%s' | awk 'END{print $1}'", fsPath)
	outputBytes, err := exec.Command("sh", "-c", getFSCommand).Output()
	if err != nil || len(outputBytes) == 0 {
		glog.Errorf("Unable to retrieve FS from fspath %s, err: %v", fsPath, err)
		return
	}
	fileSystem = strings.TrimSpace(string(outputBytes))
	return fileSystem
}

func getDiskFromFileSystem(fileSystem string) (disks StringSet) {
	getDiskCommand := exec.Command("lsblk", "-no", "name", "-J")
	outputBytes, err := getDiskCommand.Output()
	if err != nil || len(outputBytes) == 0 {
		glog.Errorf("Unable to retrieve disks from FS [%s].", fileSystem)
		return
	}

	disks = getDisksFromLsblkOutput(outputBytes, fileSystem)
	return
}

func getDisksFromLsblkOutput(lsblkOutputBytes []byte, fileSystem string) (disks StringSet) {
	disks = make(StringSet)
	device := strings.TrimPrefix(fileSystem, "/dev/")
	var lsBlkOutput LsBlk
	err := json.Unmarshal(lsblkOutputBytes, &lsBlkOutput)
	if err != nil {
		glog.Errorf("Unable to unmarshal lsblk output [%s]. Error: [%v]", string(lsblkOutputBytes), err)
		return
	}
	for _, blockDevice := range lsBlkOutput.BlockDevices {
		for _, child := range blockDevice.ChildBlockDevices {
			if child.Name == device {
				disks[blockDevice.Name] = struct{}{}
			}
		}
	}
	return
}

func getFileSystemToDiskMap() map[string]StringSet {
	fileSystemToDiskMap := make(map[string]StringSet, len(ctx.mountpaths.Available))
	for _, mountPath := range ctx.mountpaths.Available {
		disks := getDiskFromFileSystem(mountPath.FileSystem)
		if len(disks) == 0 {
			continue
		}
		fileSystemToDiskMap[mountPath.FileSystem] = disks
	}

	return fileSystemToDiskMap
}

// CheckIostatVersion determines whether iostat command is present and
// is not too old (at least version `iostatMinVersion` is required).
func CheckIostatVersion() error {
	cmd := exec.Command("iostat", "-V")

	vbytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("[iostat] Error: %v", err)
	}

	vwords := strings.Split(string(vbytes), "\n")
	if vwords = strings.Split(vwords[0], " "); len(vwords) < 3 {
		return fmt.Errorf("[iostat] Error: unknown iostat version format %v", vwords)
	}

	vss := strings.Split(vwords[2], ".")
	if len(vss) < 3 {
		return fmt.Errorf("[iostat] Error: unexpected version format: %v", vss)
	}

	version := []int64{}
	for _, vs := range vss {
		v, err := strconv.ParseInt(vs, 10, 64)
		if err != nil {
			return fmt.Errorf("[iostat] Error: failed to parse version %v", vss)
		}
		version = append(version, v)
	}

	if version[0] < iostatMinVersion {
		return fmt.Errorf("[iostat] Error: version %v is too old. At least %v version is required", version, iostatMinVersion)
	}

	return nil
}
