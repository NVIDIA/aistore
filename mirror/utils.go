// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"time"

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

func delCopies(lom *cluster.LOM, copies int) (size int64, err error) {
	lom.Lock(true)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache()
	if err := lom.Load(false); err != nil {
		return 0, err
	}

	ndel := lom.NumCopies() - copies
	if ndel <= 0 {
		lom.Unlock(true)
		return
	}

	copiesFQN := make([]string, 0, ndel)
	for copyFQN := range lom.GetCopies() {
		if copyFQN == lom.FQN {
			continue
		}
		copiesFQN = append(copiesFQN, copyFQN)
		ndel--
		if ndel == 0 {
			break
		}
	}

	size = int64(len(copiesFQN)) * lom.Size()
	if err = lom.DelCopies(copiesFQN...); err == nil {
		lom.ReCache()
	}

	lom.Unlock(true)
	return
}

func addCopies(lom *cluster.LOM, copies int, mpathers map[string]mpather, buf []byte) (size int64, err error) {
	lom.Lock(false)
	defer lom.Unlock(false)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache()
	if err := lom.Load(false); err != nil {
		return 0, err
	}

	// Recheck if we still need to create the copy.
	if lom.NumCopies() >= copies {
		return 0, nil
	}

	for i := lom.NumCopies() + 1; i <= copies; i++ {
		if mpather := findLeastUtilized(lom, mpathers); mpather != nil {
			var clone *cluster.LOM
			if clone, err = copyTo(lom, mpather.mountpathInfo(), buf); err != nil {
				glog.Errorln(err)
				return
			}
			size += lom.Size()
			if glog.FastV(4, glog.SmoduleMirror) {
				glog.Infof("copied %s=>%s", lom, clone)
			}
		} else {
			err = fmt.Errorf("%s (copies=%d): cannot find dst mountpath", lom, lom.NumCopies())
			return
		}
	}
	return
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

	copyFQN := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, bck.Name, bck.Provider, lom.Objname)
	clone, err = lom.CopyObject(copyFQN, buf)
	if err != nil {
		return
	}
	lom.ReCache()
	return
}

// static helper
func checkInsufficientMpaths(xact cmn.Xact, mpathCount int) error {
	if mpathCount < 2 {
		return fmt.Errorf("%s: number of mountpaths (%d) is insufficient for local mirroring, exiting", xact, mpathCount)
	}
	return nil
}
