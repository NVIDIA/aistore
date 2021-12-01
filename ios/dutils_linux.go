// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"flag"
	"os/exec"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	jsoniter "github.com/json-iterator/go"
)

type LsBlk struct {
	BlockDevices []*BlockDevice `json:"blockdevices"`
}

// `lsblk -Jt` structure
type BlockDevice struct {
	Name         string          `json:"name"`
	PhySec       jsoniter.Number `json:"phy-sec"`
	BlockDevices []*BlockDevice  `json:"children"`
}

// fs2disks is used when a mountpath is added to
// retrieve the disk(s) associated with a filesystem.
// This returns multiple disks only if the filesystem is RAID.
func fs2disks(fs string) (disks FsDisks) {
	getDiskCommand := exec.Command("lsblk", "-Jt")
	outputBytes, err := getDiskCommand.Output()
	if err != nil || len(outputBytes) == 0 {
		glog.Errorf("%s: no disks, err: %v", fs, err)
		return
	}
	var (
		lsBlkOutput LsBlk
		device      = strings.TrimPrefix(fs, "/dev/")
	)
	disks = make(FsDisks, 4)
	err = jsoniter.Unmarshal(outputBytes, &lsBlkOutput)
	if err != nil {
		glog.Errorf("Unable to unmarshal lsblk output [%s], err: %v", string(outputBytes), err)
		return
	}
	findDevDisks(lsBlkOutput.BlockDevices, device, disks)
	if flag.Parsed() {
		glog.Infof("%s: %+v", fs, disks)
	}
	return disks
}

//
// private
//

func childMatches(devList []*BlockDevice, device string) bool {
	for _, dev := range devList {
		if dev.Name == device {
			return true
		}
		if len(dev.BlockDevices) != 0 && childMatches(dev.BlockDevices, device) {
			return true
		}
	}
	return false
}

func findDevDisks(devList []*BlockDevice, device string, disks FsDisks) {
	addDisk := func(bd *BlockDevice) {
		var err error
		if disks[bd.Name], err = bd.PhySec.Int64(); err != nil {
			glog.Errorf("%s[%v]: %v", bd.Name, bd, err)
			disks[bd.Name] = 512
		}
	}

	for _, bd := range devList {
		if bd.Name == device {
			addDisk(bd)
			continue
		}
		if len(bd.BlockDevices) > 0 && childMatches(bd.BlockDevices, device) {
			addDisk(bd)
		}
	}
}
