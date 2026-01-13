// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"

	onexxh "github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
// See also: core/meta/hrw.go

func Hrw(uname []byte) (mi *Mountpath, digest uint64, err error) {
	avail := GetAvail()
	return avail.Hrw(uname)
}

func (avail MPI) Hrw(uname []byte) (mi *Mountpath, digest uint64, err error) {
	var (
		maxH uint64
	)
	digest = onexxh.Checksum64S(uname, cos.MLCG32)
	for _, mpathInfo := range avail {
		if mpathInfo.IsAnySet(FlagWaitingDD) {
			continue
		}
		cs := xoshiro256.Hash(mpathInfo.PathDigest ^ digest)
		if cs >= maxH {
			maxH = cs
			mi = mpathInfo
		}
	}
	if mi == nil {
		err = cmn.ErrNoMountpaths
	}
	return
}
