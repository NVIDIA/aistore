// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const ftcg = "Warning: failed to cold-GET"

// stats and redundancy (compare w/ goi.txfini)
func (goi *getOI) _fini(revert string, fullSize, txSize int64) error {
	lom := goi.lom
	if revert != "" {
		if err := cos.RemoveFile(revert); err != nil {
			nlog.InfoDepth(1, ftcg, "(rm-revert)", lom, err)
		}
	}

	// make copies and slices (async)
	if err := ec.ECM.EncodeObject(lom, nil); err != nil && err != ec.ErrorECDisabled {
		nlog.InfoDepth(1, ftcg, "(ec)", lom, err)
	}
	goi.t.putMirror(lom)

	// load
	if err := lom.Load(true /*cache it*/, true /*locked*/); err != nil {
		goi.lom.Unlock(true)
		nlog.InfoDepth(1, ftcg, "(load)", lom, err) // (unlikely)
		return cmn.ErrGetTxBenign
	}
	debug.Assert(lom.Lsize() == fullSize)
	goi.lom.Unlock(true)

	// regular get stats
	goi.stats(txSize)
	return nil
}

func (goi *getOI) _cleanup(revert string, lmfh io.Closer, buf []byte, slab *memsys.Slab, err error, tag string) {
	if lmfh != nil {
		lmfh.Close()
	}
	if slab != nil {
		slab.Free(buf)
	}
	if err == nil {
		goi.lom.Unlock(true)
		return
	}

	if isErrGetTxSevere(err) {
		goi.isIOErr = true
	}

	lom := goi.lom
	cname := lom.Cname()
	if errV := lom.RemoveMain(); errV != nil {
		nlog.Warningln("failed to remove work-main", cname, errV)
	}
	if revert != "" {
		if errV := lom.RenameToMain(revert); errV != nil {
			nlog.Warningln("failed to revert", cname, errV)
		} else {
			nlog.Infoln("reverted", cname)
		}
	}
	lom.Unlock(true)

	s := err.Error()
	if cos.IsErrBrokenPipe(err) {
		s = "[broken pipe]" // EPIPE
	}
	nlog.InfoDepth(1, ftcg, tag, cname, "err:", s)
}

// NOTE:
// Streaming cold GET feature (`feat.StreamingColdGET`) puts response header on the wire _prior_
// to finalizing in-cluster object. Use it at your own risk.
// (under wlock)
func (goi *getOI) coldStream(res *core.GetReaderResult) error {
	var (
		t, lom = goi.t, goi.lom
		revert string
	)
	if goi.verchanged {
		revert = fs.CSM.Gen(lom, fs.WorkCT, fs.WorkfileColdget)
		if err := lom.RenameMainTo(revert); err != nil {
			nlog.Errorln("failed to rename prev. version - proceeding anyway", lom.FQN, "=>", revert)
			revert = ""
		}
	}
	lmfh, err := lom.Create()
	if err != nil {
		cos.Close(res.R)
		goi._cleanup(revert, nil, nil, nil, err, "(fcreate)")
		return err
	}

	// read remote, write local, transmit --

	var (
		written   int64
		buf, slab = t.gmm.AllocSize(_txsize(res.Size))
		cksum     = cos.NewCksumHash(lom.CksumConf().Type)
		mw        = cos.NewWriterMulti(goi.w, lmfh, cksum.H)
		whdr      = goi.w.Header()
	)

	// response header
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	cmn.ToHeader(lom.ObjAttrs(), whdr, res.Size)
	if goi.dpq.isS3 {
		// (expecting user to set bucket checksum = md5)
		s3.SetS3Headers(whdr, goi.lom)
	}

	written, err = cos.CopyBuffer(mw, res.R, buf)
	cos.Close(res.R)

	if err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(rr/wl)")
		return cmn.ErrGetTxBenign
	}
	if written != res.Size {
		errTx := goi._txerr(nil, lom.FQN, written, res.Size)
		debug.Assert(isErrGetTxSevere(errTx), errTx)
		goi._cleanup(revert, lmfh, buf, slab, errTx, "(rr/wl)")
		return errTx
	}

	if lom.IsFeatureSet(feat.FsyncPUT) {
		// fsync (flush)
		if err = lmfh.Sync(); err != nil {
			const act = "(fsync)"
			errTx := newErrGetTxSevere(err, lom, act)
			goi._cleanup(revert, lmfh, buf, slab, errTx, act)
			return errTx
		}
	}

	err = lmfh.Close()
	lmfh = nil
	if err != nil {
		const act = "(fclose)"
		errTx := newErrGetTxSevere(err, lom, act)
		goi._cleanup(revert, lmfh, buf, slab, errTx, act)
		return errTx
	}

	// lom (main replica)
	lom.SetSize(written)
	cksum.Finalize()
	lom.SetCksum(&cksum.Cksum)
	if lom.HasCopies() {
		if err := lom.DelAllCopies(); err != nil {
			nlog.Errorln(err)
		}
	}
	if err = lom.PersistMain(false /*isChunked*/); err != nil {
		const act = "(persist)"
		errTx := newErrGetTxSevere(err, lom, act)
		goi._cleanup(revert, lmfh, buf, slab, errTx, act)
		return errTx
	}

	slab.Free(buf)

	return goi._fini(revert, res.Size, written)
}
