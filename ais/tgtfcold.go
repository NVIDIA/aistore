// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

const ftcg = "failed to cold-GET"

func (goi *getOI) coldSeek(res *cluster.GetReaderResult) error {
	var (
		t, lom = goi.t, goi.lom
		fqn    = lom.FQN
		revert string
	)
	if goi.verchanged {
		revert = fs.CSM.Gen(lom, fs.WorkfileType, fs.WorkfileColdget)
		if err := os.Rename(lom.FQN, revert); err != nil {
			nlog.Errorln("failed to rename prev. version - proceeding anyway", lom.FQN, "=>", revert)
			revert = ""
		}
	}
	lmfh, err := lom.CreateFileRW(fqn)
	if err != nil {
		cos.Close(res.R)
		goi._cleanup(revert, nil, nil, nil, err, "(fcreate)")
		return err
	}

	// read remote, write local
	var (
		written   int64
		buf, slab = t.gmm.AllocSize(min(res.Size, 64*cos.KiB))
		cksum     = cos.NewCksumHash(lom.CksumConf().Type)
		mw        = cos.NewWriterMulti(lmfh, cksum.H)
	)
	written, err = io.CopyBuffer(mw, res.R, buf)
	cos.Close(res.R)

	if err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(rr/wl)")
		return err
	}
	debug.Assertf(written == res.Size, "%s: expected size=%d, got %d", lom.Cname(), res.Size, written)

	// fsync (flush), if requested
	if cmn.Rom.Features().IsSet(feat.FsyncPUT) {
		if err = lmfh.Sync(); err != nil {
			goi._cleanup(revert, lmfh, buf, slab, err, "(fsync)")
			return err
		}
	}

	// persist lom (main repl.)
	lom.SetSize(written)
	if cksum != nil {
		cksum.Finalize()
		lom.SetCksum(&cksum.Cksum)
	}
	if lom.HasCopies() {
		if err := lom.DelAllCopies(); err != nil {
			nlog.Errorln(err)
		}
	}
	if err = lom.PersistMain(); err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(persist)")
		return err
	}
	// with remaining stats via goi.stats()
	goi.t.statsT.AddMany(
		cos.NamedVal64{Name: stats.GetColdCount, Value: 1},
		cos.NamedVal64{Name: stats.GetColdSize, Value: res.Size},
		cos.NamedVal64{Name: stats.GetColdRwLatency, Value: mono.SinceNano(goi.ltime)},
	)

	// fseek and r-range, if req
	if _, err = lmfh.Seek(0, io.SeekStart); err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(seek)")
		return err
	}
	var (
		hrng   *htrange
		size             = lom.SizeBytes()
		reader io.Reader = lmfh
		whdr             = goi.w.Header()
	)
	if goi.ranges.Range != "" {
		rsize := goi.lom.SizeBytes()
		if goi.ranges.Size > 0 {
			rsize = goi.ranges.Size
		}
		if hrng, _, err = goi.parseRange(whdr, rsize); err != nil {
			goi._cleanup(revert, lmfh, buf, slab, err, "(seek)")
			return err
		}
		size = hrng.Length
		reader = io.NewSectionReader(lmfh, hrng.Start, hrng.Length)
	}

	// transmit
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	cmn.ToHeader(lom.ObjAttrs(), whdr)

	w := cos.WriterOnly{Writer: io.Writer(goi.w)}
	written, err = io.CopyBuffer(w, reader, buf)
	if err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(transmit)")
		return errSendingResp
	}
	debug.Assertf(written == size, "written %d != %d exp. size", written, size)

	slab.Free(buf)
	cos.Close(lmfh)
	if revert != "" {
		if errV := cos.RemoveFile(revert); errV != nil {
			nlog.Errorln(ftcg+"(rm-revert)", lom, err)
		}
	}

	// make copies and slices (async)
	if err = ec.ECM.EncodeObject(lom); err != nil && err != ec.ErrorECDisabled {
		nlog.Errorln(ftcg+"(ec)", lom, err)
	}
	t.putMirror(lom)

	// load; inc stats
	if err = lom.Load(true /*cache it*/, true /*locked*/); err != nil {
		goi.lom.Unlock(true)
		nlog.Errorln(ftcg+"(load)", lom, err) // (unlikely)
		return errSendingResp
	}
	goi.lom.Unlock(true)

	goi.stats(written)
	return nil
}

func (goi *getOI) _cleanup(revert string, fh *os.File, buf []byte, slab *memsys.Slab, err error, tag string) {
	if fh != nil {
		fh.Close()
	}
	if slab != nil {
		slab.Free(buf)
	}
	if err != nil {
		goi.lom.Remove()
		if revert != "" {
			if errV := os.Rename(revert, goi.lom.FQN); errV != nil {
				nlog.Errorln(ftcg+tag+"(revert)", errV)
			}
		}
		nlog.ErrorDepth(1, ftcg+tag, err)
	}
	goi.lom.Unlock(true)
}
