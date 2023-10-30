// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const ftcg = "failed to cold-GET"

func (goi *getOI) coldSeek(res *cluster.GetReaderResult) error {
	var (
		cksum  *cos.CksumHash
		t, lom = goi.t, goi.lom
		ckconf = lom.CksumConf()
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
		buf, slab = t.gmm.AllocSize(res.Size)
	)
	if ckconf.Type == cos.ChecksumNone {
		written, err = io.CopyBuffer(lmfh, res.R, buf)
	} else {
		cksum = cos.NewCksumHash(ckconf.Type)
		mw := cos.NewWriterMulti(lmfh, cksum.H)
		written, err = io.CopyBuffer(mw, res.R, buf)
	}
	cos.Close(res.R)

	if err == nil && written != res.Size && res.Size > 0 {
		err = fmt.Errorf("expected: %s size %d, got %d", lom.Cname(), res.Size, written) // (unlikely)
	}
	if err != nil {
		goi._cleanup(revert, lmfh, buf, slab, err, "(rr/wl)")
		return err
	}

	// fsync, if requested
	if cmn.Rom.Features().IsSet(feat.FsyncPUT) {
		if err = lmfh.Sync(); err != nil {
			goi._cleanup(revert, lmfh, buf, slab, err, "(fsync)")
			return err
		}
	}

	// persist lom (main repl)
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

	// seek start and transmit
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
	if err != nil || written != size {
		if err == nil {
			err = fmt.Errorf("written %d != %d exp. size", written, size)
		}
		goi._cleanup(revert, lmfh, buf, slab, err, "(transmit)")
		return errSendingResp
	}

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
		goi._cleanup("", nil, nil, nil, err, "(load)") // (unlikely)
		return err
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
