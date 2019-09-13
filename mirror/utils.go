// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"time"

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
		util       int64 = 101
		skip             = make(cmn.SimpleKVs)
		now              = time.Now()
		mpathUtils       = fs.Mountpaths.GetAllMpathUtils(now)
	)
loop:
	for _, j := range mpathers {
		jpath := j.mountpathInfo().Path
		if jpath == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if lom.HasCopies() {
			// skip existing
			for cpyfqn, mpathInfo := range lom.GetCopies() {
				cpath, ok := skip[cpyfqn]
				if !ok {
					cpath = mpathInfo.Path
					skip[cpyfqn] = cpath
				}
				if jpath == cpath {
					continue loop
				}
			}
		}
		if u, ok := mpathUtils[jpath]; ok && u < util {
			out = j
			util = u
		}
	}
	return
}

func copyTo(lom *cluster.LOM, mpathInfo *fs.MountpathInfo, buf []byte) (clone *cluster.LOM, err error) {
	orig := lom.ParsedFQN.MpathInfo
	parsedFQN := lom.ParsedFQN
	parsedFQN.MpathInfo = mpathInfo
	workFQN := fs.CSM.GenContentParsedFQN(parsedFQN, fs.WorkfileType, fs.WorkfilePut)
	copyFQN := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, lom.IsAIS(), lom.Bucket(), lom.Objname)

	_, err = lom.CopyObject(copyFQN, workFQN, buf, true /*dstIsCopy*/, false /*srcCopyOK=false*/)
	if err != nil {
		return
	}
	lom.AddCopy(copyFQN, mpathInfo)
	if err = lom.Persist(); err == nil {
		clone = lom.Clone(copyFQN)
		clone.HrwFQN = lom.FQN
		clone.SetCopies(lom.FQN, orig)
		if err = clone.Persist(); err == nil {
			lom.ReCache()
			return // ok
		}
	}
	// on error
	_ = lom.DelCopy(copyFQN)
	return
}
