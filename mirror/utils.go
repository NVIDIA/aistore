// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
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
	var (
		util int64 = 101
		skip       = make(cmn.SimpleKVs)
	)
loop:
	for _, j := range mpathers {
		jpath := j.mountpathInfo().Path
		if jpath == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if lom.HasCopies() {
			// skip existing
			for _, cpyfqn := range lom.CopyFQN() {
				cpath, ok := skip[cpyfqn]
				if !ok {
					parsedFQN, err := fs.Mountpaths.FQN2Info(cpyfqn) // can be optimized via lom.init
					if err != nil {
						glog.Errorf("%s: failed to parse copyFQN %s, err: %v", lom, cpyfqn, err)
						continue loop
					}
					cpath = parsedFQN.MpathInfo.Path
					skip[cpyfqn] = cpath
				}
				if jpath == cpath {
					continue loop
				}
			}
		}
		if u := fs.Mountpaths.Iostats.GetDiskUtil(jpath); u < util {
			out = j
			util = u
		}
	}
	return
}

func copyTo(lom *cluster.LOM, mpathInfo *fs.MountpathInfo, buf []byte) (err error) {
	parsedFQN := lom.ParsedFQN
	parsedFQN.MpathInfo = mpathInfo
	workFQN := fs.CSM.GenContentParsedFQN(parsedFQN, fs.WorkfileType, fs.WorkfilePut)

	_, err = lom.CopyObject(workFQN, buf)
	if err != nil {
		return
	}

	cpyFQN := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, lom.BckIsLocal, lom.Bucket, lom.Objname)

	if err = cmn.MvFile(workFQN, cpyFQN); err != nil {
		if errRemove := os.Remove(workFQN); errRemove != nil {
			glog.Errorf("Failed to remove %s, err: %v", workFQN, errRemove)
		}
		return
	}

	// Append copyFQN to FQNs of existing copies
	lom.AddXcopy(cpyFQN)

	if err = lom.Persist(); err == nil {
		copyLOM := lom.Clone(cpyFQN)
		copyLOM.SetCopyFQN([]string{lom.FQN})
		if err = copyLOM.Persist(); err == nil {
			lom.ReCache()
			return
		}
	}

	// on error
	// FIXME: add rollback which restores lom's metadata in case of failure
	if err := os.Remove(cpyFQN); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.FQN)
	}

	lom.ReCache()
	return
}
