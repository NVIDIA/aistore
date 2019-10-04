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
		copiesMpath cmn.StringSet

		util       int64 = 101
		now              = time.Now()
		mpathUtils       = fs.Mountpaths.GetAllMpathUtils(now)
	)

	if lom.HasCopies() {
		copiesMpath = make(cmn.StringSet)
		for _, mpathInfo := range lom.GetCopies() {
			copiesMpath.Add(mpathInfo.Path)
		}
	}

	for _, j := range mpathers {
		jpath := j.mountpathInfo().Path
		if jpath == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if lom.HasCopies() {
			// skip existing
			if copiesMpath.Contains(jpath) {
				continue
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
	var (
		bck = lom.Bck()
	)

	// Reload metadata, it is necessary to have it fresh
	lom.Uncache()
	if err := lom.Load(false); err != nil {
		return nil, err
	}

	copyFQN := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, bck.Name, bck.Provider, lom.Objname)
	clone, err = lom.CopyObject(copyFQN, buf)
	if err != nil {
		return
	}
	if err = lom.Persist(); err != nil {
		_ = lom.DelCopy(copyFQN)
		return
	}

	lom.ReCache()
	return
}
