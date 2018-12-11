// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"os/exec"
	"strconv"
	"strings"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/json-iterator/go"
)

type LsBlk struct {
	BlockDevices []BlockDevice `json:"blockdevices"`
}

type BlockDevice struct {
	Name         string        `json:"name"`
	BlockDevices []BlockDevice `json:"children"`
}

//
// private
//

// This method is used when starting iostat runner to
// retrieve the disks associated with a filesystem.
func fs2disks(fs string) (disks cmn.StringSet) {
	getDiskCommand := exec.Command("lsblk", "-no", "name", "-J")
	outputBytes, err := getDiskCommand.Output()
	if err != nil || len(outputBytes) == 0 {
		glog.Errorf("Unable to retrieve disks from FS [%s].", fs)
		return
	}

	disks = lsblkOutput2disks(outputBytes, fs)
	return
}

func childMatches(devList []BlockDevice, device string) bool {
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

func findDevDisks(devList []BlockDevice, device string, disks cmn.StringSet) {
	for _, bd := range devList {
		if bd.Name == device {
			disks[bd.Name] = struct{}{}
			continue
		}
		if len(bd.BlockDevices) != 0 {
			if childMatches(bd.BlockDevices, device) {
				disks[bd.Name] = struct{}{}
			}
		}
	}
}

func lsblkOutput2disks(lsblkOutputBytes []byte, fs string) (disks cmn.StringSet) {
	disks = make(cmn.StringSet)
	device := strings.TrimPrefix(fs, "/dev/")
	var lsBlkOutput LsBlk
	err := jsoniter.Unmarshal(lsblkOutputBytes, &lsBlkOutput)
	if err != nil {
		glog.Errorf("Unable to unmarshal lsblk output [%s]. Error: [%v]", string(lsblkOutputBytes), err)
		return
	}

	findDevDisks(lsBlkOutput.BlockDevices, device, disks)
	if glog.V(3) {
		glog.Infof("Device: %s, disk list: %v\n", device, disks)
	}

	return disks
}

func maxUtilDisks(disksMetricsMap map[string]cmn.SimpleKVs, disks cmn.StringSet) (maxutil float64) {
	maxutil = -1
	util := func(disk string) (u float64) {
		if ioMetrics, ok := disksMetricsMap[disk]; ok {
			if utilStr, ok := ioMetrics["%util"]; ok {
				var err error
				if u, err = strconv.ParseFloat(utilStr, 32); err == nil {
					return
				}
			}
		}
		return
	}
	if len(disks) > 0 {
		for disk := range disks {
			if u := util(disk); u > maxutil {
				maxutil = u
			}
		}
		return
	}
	for disk := range disksMetricsMap {
		if u := util(disk); u > maxutil {
			maxutil = u
		}
	}
	return
}
