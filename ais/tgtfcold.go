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

func (goi *getOI) coldReopen(res *core.GetReaderResult) error {
	var (
		err    error
		lmfh   cos.LomReader
		wfh    cos.LomWriter
		t      = goi.t
		lom    = goi.lom
		revert string
	)
	if goi.verchanged {
		revert = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileColdget)
		if errV := lom.RenameMainTo(revert); errV != nil {
			nlog.Errorln("failed to rename prev. version - proceeding anyway", lom.Cname(), "=>", revert)
			revert = ""
		}
	}
	wfh, err = lom.Create()
	if err != nil {
		cos.Close(res.R)
		goi._cleanup(revert, nil, nil, nil, err, "(fcreate)")
		return err
	}

	// read remote, write local
	var (
		written   int64
		buf, slab = t.gmm.AllocSize(min(res.Size, memsys.DefaultBuf2Size))
		cksumH    = cos.NewCksumHash(lom.CksumConf().Type)
		mw        = cos.NewWriterMulti(wfh, cksumH.H)
	)
	written, err = cos.CopyBuffer(mw, res.R, buf)
	cos.Close(res.R)

	if err != nil {
		goi._cleanup(revert, wfh, buf, slab, err, "(rr/wl)")
		return err
	}
	debug.Assertf(written == res.Size, "%s: remote-size %d != %d written", lom.Cname(), res.Size, written)

	if lom.IsFeatureSet(feat.FsyncPUT) {
		// fsync (flush)
		if err = wfh.Sync(); err != nil {
			goi._cleanup(revert, wfh, buf, slab, err, "(fsync)")
			return err
		}
	}
	err = wfh.Close()
	wfh = nil
	if err != nil {
		goi._cleanup(revert, wfh, buf, slab, err, "(fclose)")
		return err
	}

	// lom (main replica)
	lom.SetSize(written)
	cksumH.Finalize()
	lom.SetCksum(&cksumH.Cksum) // and return via whdr as well
	if lom.HasCopies() {
		if err := lom.DelAllCopies(); err != nil {
			nlog.Errorln(err)
		}
	}
	if err = lom.PersistMain(); err != nil {
		goi._cleanup(revert, wfh, buf, slab, err, "(persist)")
		return err
	}

	// reopen & transmit ---
	lmfh, err = lom.Open()
	if err != nil {
		goi._cleanup(revert, nil, buf, slab, err, "(seek)")
		return err
	}
	var (
		errTx  error
		hrng   *htrange
		size             = lom.Lsize()
		reader io.Reader = lmfh
		whdr             = goi.w.Header()
	)
	if goi.ranges.Range != "" {
		// (not here if range checksum enabled)
		rsize := goi.lom.Lsize()
		if goi.ranges.Size > 0 {
			rsize = goi.ranges.Size
		}
		if hrng, _, err = goi.rngToHeader(whdr, rsize); err != nil {
			goi._cleanup(revert, lmfh, buf, slab, err, "(seek)")
			return err
		}
		size = hrng.Length
		reader = io.NewSectionReader(lmfh, hrng.Start, hrng.Length)
	}

	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	cmn.ToHeader(lom.ObjAttrs(), whdr, size)
	if goi.dpq.isS3 {
		// (expecting user to set bucket checksum = md5)
		s3.SetS3Headers(whdr, goi.lom)
	}

	written, err = cos.CopyBuffer(goi.w, reader, buf)
	if err != nil || written != size {
		errTx = goi._txerr(err, goi.lom.FQN, written, size)
	}
	if errTx != nil && errTx != errGetTxBenign {
		goi._cleanup(revert, lmfh, buf, slab, err, "(transmit)")
		return errTx
	}

	cos.Close(lmfh)
	slab.Free(buf)

	// apc.QparamIsGFNRequest: update GFN filter to skip _rebalancing_ this one
	if goi.dpq.isGFN {
		bname := cos.UnsafeBptr(lom.UnamePtr())
		goi.t.reb.FilterAdd(*bname)
	}

	err = goi._fini(revert, res.Size, written)
	if err == nil {
		err = errTx
	}
	return err
}

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
		return errGetTxBenign
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
		revert = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileColdget)
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
		buf, slab = t.gmm.AllocSize(min(res.Size, memsys.DefaultBuf2Size))
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
		return errGetTxBenign
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
	if err = lom.PersistMain(); err != nil {
		const act = "(persist)"
		errTx := newErrGetTxSevere(err, lom, act)
		goi._cleanup(revert, lmfh, buf, slab, errTx, act)
		return errTx
	}

	slab.Free(buf)

	return goi._fini(revert, res.Size, written)
}
