// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

// is under lock
func delCopies(lom *core.LOM, copies int) (size int64, err error) {
	// force reloading metadata
	lom.UncacheUnless()
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

// under LOM's w-lock => TODO: a finer-grade mechanism to write-protect
// metadata only, md.copies in this case
func addCopies(lom *core.LOM, copies int, buf []byte) (size int64, err error) {
	// Reload metadata, it is necessary to have it fresh.
	lom.UncacheUnless()
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
		var mi *fs.Mountpath
		if mi = lom.LeastUtilNoCopy(); mi == nil {
			err = fmt.Errorf("%s (copies=%d): cannot find dst mountpath", lom, lom.NumCopies())
			return
		}
		if err = lom.Copy(mi, buf); err != nil {
			nlog.Errorln(err)
			return
		}
		size += lom.SizeBytes()
	}
	return
}

func drainWorkCh(workCh chan core.LIF) (n int) {
	for {
		select {
		case <-workCh:
			n++
		default:
			return
		}
	}
}
