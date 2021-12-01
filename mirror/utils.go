// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/fs"
)

func delCopies(lom *cluster.LOM, copies int) (size int64, err error) {
	lom.Lock(true)
	defer lom.Unlock(true)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache(false /*delDirty*/)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
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

	size = int64(len(copiesFQN)) * lom.SizeBytes()
	if err = lom.DelCopies(copiesFQN...); err != nil {
		return
	}
	err = lom.Persist()
	return
}

func addCopies(lom *cluster.LOM, copies int, buf []byte) (size int64, err error) {
	// TODO: finer-grade mechanism to write-protect metadata only (md.copies in this case)
	lom.Lock(true)
	defer lom.Unlock(true)

	// Reload metadata, it is necessary to have it fresh.
	lom.Uncache(false /*delDirty*/)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return 0, err
	}

	// Recheck if we still need to create the copy.
	if lom.NumCopies() >= copies {
		return 0, nil
	}

	//  While copying we may find out that some copies do not exist -
	//  these copies will be removed and `NumCopies()` will decrease.
	for lom.NumCopies() < copies {
		var (
			mi    *fs.MountpathInfo
			clone *cluster.LOM
		)
		if mi = lom.BestMpath(); mi == nil {
			err = fmt.Errorf("%s (copies=%d): cannot find dst mountpath", lom, lom.NumCopies())
			return
		}
		copyFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
		if clone, err = lom.CopyObject(copyFQN, buf); err != nil {
			glog.Errorln(err)
			cluster.FreeLOM(clone)
			return
		}
		size += lom.SizeBytes()
		cluster.FreeLOM(clone)
	}
	return
}

func drainWorkCh(workCh chan cluster.LIF) (n int) {
	for {
		select {
		case <-workCh:
			n++
		default:
			return
		}
	}
}
