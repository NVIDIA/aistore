// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"os"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact/xs"
)

func (t *target) Promote(params *core.PromoteParams) (ecode int, err error) {
	lom := core.AllocLOM(params.ObjName)
	if err = lom.InitBck(params.Bck.Bucket()); err == nil {
		ecode, err = t._promote(params, lom)
	}
	core.FreeLOM(lom)
	return ecode, err
}

func (t *target) _promote(params *core.PromoteParams, lom *core.LOM) (ecode int, err error) {
	smap := t.owner.smap.get()
	tsi, local, erh := lom.HrwTarget(&smap.Smap)
	if erh != nil {
		return 0, erh
	}
	var size int64
	if local {
		size, ecode, err = t._promLocal(params, lom)
	} else {
		size, err = t._promRemote(params, lom, tsi, smap)
		if err == nil && size >= 0 && params.Xact != nil {
			params.Xact.OutObjsAdd(1, size)
		}
	}
	if err != nil {
		return ecode, err
	}
	if size >= 0 && params.Xact != nil {
		params.Xact.ObjsAdd(1, size) // (as initiator)
	}
	if params.DeleteSrc {
		if errRm := cos.RemoveFile(params.SrcFQN); errRm != nil {
			nlog.Errorf("%s: failed to remove promoted source %q: %v", t, params.SrcFQN, errRm)
		}
	}
	return 0, nil
}

// return fileSize = -1 not to be counted in stats
func (t *target) _promLocal(params *core.PromoteParams, lom *core.LOM) (fileSize int64, _ int, _ error) {
	var (
		cksum     *cos.CksumHash
		workFQN   string
		extraCopy = true
	)
	if err := lom.Load(true /*cache it*/, false /*locked*/); err == nil && !params.OverwriteDst {
		return -1, 0, nil
	}
	if params.DeleteSrc {
		// To use `params.SrcFQN` as `workFQN`, make sure both are
		// located on the same filesystem. About "filesystem sharing" see also:
		// * https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#terminology
		mi, _, err := fs.FQN2Mpath(params.SrcFQN)
		extraCopy = err != nil || !mi.FS.Equal(lom.Mountpath().FS)
	}
	if extraCopy {
		var (
			buf, slab = t.gmm.Alloc()
			err       error
		)
		workFQN = lom.GenFQN(fs.WorkCT, fs.WorkfilePut)
		fileSize, cksum, err = cos.CopyFile(params.SrcFQN, workFQN, buf, lom.CksumType())
		slab.Free(buf)
		if err != nil {
			return 0, 0, err
		}
		lom.SetCksum(cksum.Clone())
	} else {
		// avoid extra copy: use the source as `workFQN`
		fi, err := os.Stat(params.SrcFQN)
		if err != nil {
			if cos.IsNotExist(err) {
				err = nil
			}
			return -1, 0, err
		}

		fileSize = fi.Size()
		workFQN = params.SrcFQN
		if params.Cksum != nil {
			lom.SetCksum(params.Cksum) // already computed somewhere else, use it
		} else {
			clone := lom.CloneTo(params.SrcFQN)
			if cksum, err = clone.ComputeCksum(lom.CksumType(), false); err != nil {
				core.FreeLOM(clone)
				return 0, 0, err
			}
			lom.SetCksum(cksum.Clone())
			core.FreeLOM(clone)
		}
	}

	if params.Cksum != nil && !cos.NoneH(cksum) {
		if !cksum.Equal(params.Cksum) {
			return 0, 0, cos.NewErrDataCksum(
				cksum.Clone(),
				params.Cksum,
				params.SrcFQN+" => "+lom.String() /*detail*/)
		}
	}

	poi := allocPOI()
	{
		poi.atime = time.Now().UnixNano()
		poi.t = t
		poi.config = params.Config
		poi.lom = lom
		poi.workFQN = workFQN
		poi.owt = cmn.OwtPromote
		poi.xctn = params.Xact
	}
	lom.SetSize(fileSize)
	ecode, err := poi.finalize()
	freePOI(poi)

	return fileSize, ecode, err
}

// [TODO]
// - use DM streams
// - Xact.InObjsAdd on the receive side
func (t *target) _promRemote(params *core.PromoteParams, lom *core.LOM, tsi *meta.Snode, smap *smapX) (int64, error) {
	lom.FQN = params.SrcFQN

	// when not overwriting check w/ remote target first (and separately)
	if !params.OverwriteDst && t.headt2t(lom, tsi, smap) {
		return -1, nil
	}

	coiParams := xs.AllocCOI()
	{
		coiParams.BckTo = lom.Bck()
		coiParams.OWT = cmn.OwtPromote
		coiParams.Xact = params.Xact
		coiParams.Config = params.Config
		coiParams.ObjnameTo = lom.ObjName
		coiParams.OAH = lom
	}
	coi := (*coi)(coiParams)

	// not opening LOM here - opening params.SrcFQN source (to be promoted)
	fh, err := cos.NewFileHandle(lom.FQN)
	if err != nil {
		if cos.IsNotExist(err) {
			return 0, err
		}
		return 0, cmn.NewErrFailedTo(t, "open", params.SrcFQN, err)
	}
	fi, err := fh.Stat()
	if err != nil {
		fh.Close()
		return 0, cmn.NewErrFailedTo(t, "fstat", params.SrcFQN, err)
	}

	res := coi.send(t, nil /*DM*/, lom, fh, tsi)
	xs.FreeCOI(coiParams)

	return fi.Size(), res.Err
}
