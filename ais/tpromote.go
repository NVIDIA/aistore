// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
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
	if err = lom.InitBck(params.Bck); err == nil {
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
		var errRm error
		if params.SrcRoot == nil {
			errRm = cos.RemoveFile(params.SrcFQN)
		} else {
			errRm = fs.RemoveNoSymlinks(params.SrcRoot, params.SrcFQN)
		}
		if errRm != nil && !cos.IsNotExist(errRm) {
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
	if params.DeleteSrc && params.SrcRoot == nil {
		// to (optimally) use `params.SrcFQN` as `workFQN`, make sure both are on the same FS
		// (see also: https://github.com/NVIDIA/aistore/blob/main/docs/terminology.md#mountpath)
		mi, _, err := fs.FQN2Mpath(params.SrcFQN)
		extraCopy = err != nil || !mi.FS.Equal(lom.Mountpath().FS)
	}
	if extraCopy {
		var (
			buf, slab = t.gmm.Alloc()
			err       error
		)
		workFQN = lom.GenFQN(fs.WorkCT, fs.WorkfilePut)
		fh, errOpen := openPromoteSource(params)
		if errOpen != nil {
			slab.Free(buf)
			return 0, 0, errOpen
		}
		fileSize, cksum, err = cos.CopyReader(fh, workFQN, buf, lom.CksumType())
		cos.Close(fh)
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
				if params.SrcMustExist {
					return -1, http.StatusNotFound, err
				}
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
	if !params.OverwriteDst {
		if _, errH := t.headt2t(lom, tsi, smap, nil /*reqProps*/); errH == nil {
			return -1, nil
		}
	}

	coiParams := xs.AllocCOI()
	defer xs.FreeCOI(coiParams)
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
	fh, err := openPromoteSource(params)
	if err != nil {
		if cos.IsNotExist(err) {
			return 0, err
		}
		return 0, cmn.NewErrFailedTo(t, "open", params.SrcFQN, err)
	}
	defer fh.Close()
	fi, err := fh.Stat()
	if err != nil {
		return 0, cmn.NewErrFailedTo(t, "fstat", params.SrcFQN, err)
	}
	lom.SetSize(fi.Size())

	// reopen/retry reads the same descriptor, never the source pathname
	reader := cos.NewSectionHandle(fh, 0, fi.Size(), 0)
	res := coi.send(t, nil /*DM*/, lom, reader, tsi)

	return fi.Size(), res.Err
}

// open API sources beneath their root; internal callers (append-flush) own their workfiles
func openPromoteSource(params *core.PromoteParams) (*os.File, error) {
	if params.SrcRoot == nil {
		return os.Open(params.SrcFQN)
	}
	return fs.OpenNoSymlinks(params.SrcRoot, params.SrcFQN)
}
