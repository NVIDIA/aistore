// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"errors"
	"flag"
	"fmt"
	"os/exec"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	jsoniter "github.com/json-iterator/go"
)

//
// Parse `lsblk -Jt` to associate filesystem (`fs`) with its disks
//

// NOTE: these are the two distinct prefixes we currently recognize (TODO: support w/ reference)
const (
	devPrefixReg = "/dev/"
	devPrefixLVM = "/dev/mapper/"
)

type (
	LsBlk struct {
		BlockDevices []*blkdev `json:"blockdevices"`
	}

	// `lsblk -Jt` structure
	blkdev struct {
		Name         string          `json:"name"`
		PhySec       jsoniter.Number `json:"phy-sec"`
		BlockDevices []*blkdev       `json:"children"`
	}
)

func lsblk(fs string, testingEnv bool) (res *LsBlk) {
	// skip docker union mounts
	if fs == "overlay" {
		return
	}
	var (
		cmd      = exec.Command("lsblk", "-Jt") // JSON output format (TODO: '-e7' to skip loopbacks)
		out, err = cmd.CombinedOutput()
	)
	if err != nil {
		switch {
		case len(out) == 0:
		case strings.Contains(err.Error(), "exit status"):
			err = errors.New(string(out))
		default:
			err = fmt.Errorf("%s: %v", string(out), err)
		}
		if !testingEnv {
			cos.ExitLog(err) // FATAL
		}
		nlog.Errorln(err)
		return
	}
	if len(out) == 0 {
		nlog.Errorf("%s: no disks (empty lsblk output)", fs)
		return
	}

	// unmarshal
	res = &LsBlk{}
	if err = jsoniter.Unmarshal(out, res); err != nil {
		err = fmt.Errorf("failed to unmarshal lsblk output: %v", err)
		if !testingEnv {
			cos.ExitLog(err) // FATAL
		}
		nlog.Errorln(err)
		res = nil
	}
	return
}

// given parsed lsblk and `fs` (filesystem) fs2disks retrieves the underlying
// disk or disks; it may return multiple disks but only if the filesystem is
// RAID; it is called upong adding/enabling mountpath
func fs2disks(res *LsBlk, fs, fspInfo string, num int, testingEnv bool) (disks FsDisks) {
	var (
		trimmedFS string
		s         string
	)
	if strings.HasPrefix(fs, devPrefixLVM) {
		trimmedFS = strings.TrimPrefix(fs, devPrefixLVM)
	} else {
		trimmedFS = strings.TrimPrefix(fs, devPrefixReg)
	}
	disks = make(FsDisks, num)
	findDevs(res.BlockDevices, trimmedFS, disks) // map trimmed(fs) <= disk(s)

	// log
	if !flag.Parsed() {
		return disks
	}
	if fspInfo != "" {
		s = ", " + fspInfo // from config.LocalConfig.FSP, if defined
	}
	if len(disks) == 0 {
		// skip err logging block devices when running with `test_fspaths` (config.TestingEnv() == true)
		// e.g.: testing with docker `/dev/root` mount with no disks
		// see also: `allowSharedDisksAndNoDisks`
		if !testingEnv {
			s, _ := jsoniter.MarshalIndent(res.BlockDevices, "", " ")
			nlog.Errorf("No disks for %s(%q%s):\n%s", fs, trimmedFS, s, string(s))
		}
	} else {
		nlog.Infoln("["+fs+s+"]:", disks._str())
	}
	return disks
}

//
// private
//

func findDevs(devList []*blkdev, trimmedFS string, disks FsDisks) {
	for _, bd := range devList {
		if bd.Name == trimmedFS {
			_add(bd, disks)
			continue
		}
		if len(bd.BlockDevices) > 0 && _match(bd.BlockDevices, trimmedFS) {
			_add(bd, disks)
		}
	}
}

func _add(bd *blkdev, disks FsDisks) {
	var err error
	if disks[bd.Name], err = bd.PhySec.Int64(); err != nil {
		nlog.Errorf("%s[%v]: failed to parse sector: %v", bd.Name, bd, err)
		disks[bd.Name] = 512
	}
}

func _match(devList []*blkdev, device string) bool {
	for _, dev := range devList {
		if dev.Name == device {
			return true
		}
		// recurs
		if len(dev.BlockDevices) != 0 && _match(dev.BlockDevices, device) {
			return true
		}
	}
	return false
}

func (disks FsDisks) _str() string {
	s := fmt.Sprintf("%v", disks)
	return strings.TrimPrefix(s, "map")
}
