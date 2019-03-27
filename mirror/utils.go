// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"errors"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type mpather interface {
	mountpathInfo() *fs.MountpathInfo
	stop()
	post(lom *cluster.LOM)
}

func findLeastUtilized(lom *cluster.LOM, mpathers map[string]mpather) (out mpather) {
	var util = cmn.PairF32{101, 101}
loop:
	for _, j := range mpathers {
		mpathInfo := j.mountpathInfo()
		if mpathInfo.Path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if lom.HasCopies() {
			for _, cpyfqn := range lom.CopyFQN {
				parsedFQN, err := fs.Mountpaths.FQN2Info(cpyfqn) // can be optimized via lom.init
				if err != nil {
					glog.Errorf("%s: failed to parse copyFQN %s, err: %v", lom, cpyfqn, err)
					continue loop
				}
				if mpathInfo.Path == parsedFQN.MpathInfo.Path {
					continue loop
				}
			}
		}
		if _, u := mpathInfo.GetIOstats(fs.StatDiskUtil); u.Max < util.Max && u.Min <= util.Min {
			out = j
			util = u
		}
	}
	return
}

func copyTo(lom *cluster.LOM, mpathInfo *fs.MountpathInfo, buf []byte) (err error) {
	mp := lom.ParsedFQN.MpathInfo
	lom.ParsedFQN.MpathInfo = mpathInfo // to generate work fname
	workFQN := lom.GenFQN(fs.WorkfileType, fs.WorkfilePut)
	lom.ParsedFQN.MpathInfo = mp

	if err = lom.CopyObject(workFQN, buf); err != nil {
		return
	}
	cpyFQN := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, lom.BckIsLocal, lom.Bucket, lom.Objname)
	if err = cmn.MvFile(workFQN, cpyFQN); err != nil {
		if errRemove := os.Remove(workFQN); errRemove != nil {
			glog.Errorf("Failed to remove %s, err: %v", workFQN, errRemove)
		}
		return
	}
	if errstr := lom.SetXcopy(cpyFQN); errstr != "" {
		err = errors.New(errstr)
	}
	return
}
