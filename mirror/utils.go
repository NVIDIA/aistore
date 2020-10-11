// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
)

func delCopies(lom *cluster.LOM, copies int) (size int64, err error) {
	lom.Lock(true)
	defer lom.Unlock(true)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache()
	if err := lom.Load(false); err != nil {
		return 0, err
	}

	ndel := lom.NumCopies() - copies
	if ndel <= 0 {
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
	if err = lom.DelCopies(copiesFQN...); err != nil {
		return
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()
	return
}

func addCopies(lom *cluster.LOM, copies int, buf []byte) (size int64, err error) {
	// TODO: We could potentially change it to `lom.Lock(false)` if we would
	//  take an exclusive lock for updating metadata (but for now we dont have
	//  mechanism for locking only metadata so we need to lock exclusively).
	lom.Lock(true)
	defer lom.Unlock(true)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache()
	if err := lom.Load(false); err != nil {
		return 0, err
	}

	// Recheck if we still need to create the copy.
	if lom.NumCopies() >= copies {
		return 0, nil
	}

	// Duplicate lom.md.copies for write access.
	lom.CloneCopiesMd()

	//  While copying we may find out that some copies do not exist -
	//  these copies will be removed and `NumCopies()` will decrease.
	for lom.NumCopies() < copies {
		if mpathInfo := findLeastUtilized(lom); mpathInfo != nil {
			var clone *cluster.LOM
			if clone, err = copyTo(lom, mpathInfo, buf); err != nil {
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
	lom.ReCache()
	return
}

func findLeastUtilized(lom *cluster.LOM) (out *fs.MountpathInfo) {
	var (
		copiesMpath cmn.StringSet

		minUtil    = int64(101)
		mpathUtils = fs.GetAllMpathUtils(mono.NanoTime())
		mpaths, _  = fs.Get()
	)

	if lom.HasCopies() {
		copiesMpath = make(cmn.StringSet)
		for _, mpathInfo := range lom.GetCopies() {
			copiesMpath.Add(mpathInfo.Path)
		}
	}

	for mpath, mpathInfo := range mpaths {
		if mpath == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		if lom.HasCopies() {
			if copiesMpath.Contains(mpath) { // Skip existing copies.
				continue
			}
		}
		if curUtil, ok := mpathUtils[mpath]; ok && curUtil < minUtil {
			minUtil = curUtil
			out = mpathInfo
		} else if !ok && out == nil {
			// Since we cannot relay on the fact that `mpathUtils` has all mountpaths
			// (because it might not refresh it now) we randomly pick a mountpath.
			out = mpathInfo
		}
	}
	return
}

func copyTo(lom *cluster.LOM, mpathInfo *fs.MountpathInfo, buf []byte) (clone *cluster.LOM, err error) {
	copyFQN := fs.CSM.FQN(mpathInfo, lom.Bck().Bck, lom.ParsedFQN.ContentType, lom.ObjName)
	clone, err = lom.CopyObject(copyFQN, buf)
	if err != nil {
		return
	}
	return
}

func drainWorkCh(workCh chan *cluster.LOM) (n int) {
	for {
		select {
		case <-workCh:
			n++
		default:
			return
		}
	}
}
