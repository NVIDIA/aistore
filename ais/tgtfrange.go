// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	onexxh "github.com/OneOfOne/xxhash"
)

// feat.RangeColdGET: ranged GET of an object that is not present in the cluster.
//
// Instead of the full-blown cold GET (that reads and stores the entire object), read
// the requested byte range from the remote backend and transmit it to the user. On the
// way out, cache it: the object is mapped onto a fixed grid of chunks; the grid-aligned
// chunks covering the range are stored and recorded in a _partial_ chunk-manifest
// (see core.Ufest). Once the grid is fully covered the manifest gets completed - at
// which point the object becomes a regular (chunked) in-cluster object.
//
// Notes:
// - the grid is fixed: chunk number <=> object offset is a bijection, and so chunks
//   accumulated out of order and with gaps require no extra metadata
// - manifest ID is derived from the remote version/ETag: when the source changes so
//   does the ID, whereby previously accumulated chunks are ignored (and, eventually,
//   removed by `ais space-cleanup`)
// - caching is best-effort and gets skipped when: the remote object has no
//   version/ETag; `validate_cold_get` is on (a ranged read carries no whole-object
//   checksum to validate); another request is already filling the same object
//
// Compare w/ goi.coldStream (feat.StreamingColdGET).

const (
	// minimum grid granularity (the tradeoff: read amplification vs. chunk count)
	rcChunkSize = 8 * cos.MiB

	rcIDprefix = "rcget-"
)

// at most one filling request per object
var rcFilling sync.Map // lom.Uname() => struct{}

type (
	// a piece of the requested range: `size` bytes at `off`
	// - path != "":  locally cached chunk
	// - count > 0:   to read [coff, coff+clen) from the backend and store it as
	//                chunks [num, num+count) - a single read for the entire run
	// - otherwise:   to read [off, off+size) from the backend, not storing anything
	rcseg struct {
		path       string
		num, count int
		coff, clen int64
		off, size  int64
	}
	rcsrc struct {
		r  io.Reader
		cl io.Closer
	}
	rcache struct {
		goi       *getOI
		u         *core.Ufest // partial manifest; nil when not caching
		uname     string
		size      int64 // remote object size
		chunkSize int64
		total     int // total chunks in the grid
		filling   bool
		added     bool
	}
)

// (under rlock)
func (goi *getOI) coldRange() (ecode int, err error) {
	var (
		t, lom = goi.t, goi.lom
		oa     *cmn.ObjAttrs
	)
	// HEAD remote object: need size to resolve the range and set Content-Range
	oa, ecode, err = t.HeadCold(lom, goi.req)
	if err != nil {
		return ecode, err
	}

	whdr := goi.w.Header()
	hrng, ecode, err := goi.rngToHeader(whdr, oa.Size)
	if err != nil {
		return ecode, err
	}
	if hrng == nil || hrng.Length <= 0 { // (empty range spec, zero-length suffix)
		return http.StatusRequestedRangeNotSatisfiable, cos.NewErrRangeNotSatisfiable(nil, []string{goi.ranges.Range}, oa.Size)
	}

	goi.cold = true
	lom.SetCustomMD(oa.GetCustomMD())
	lom.CopyVersion(oa)

	rc := goi.rcInit(oa)
	defer rc.fini()

	segs := rc.plan(hrng)

	// resolve the first segment _prior_ to putting the response header on the wire
	// (to keep reporting backend errors as such)
	src, ecode, err := rc.open(&segs[0])
	if err != nil {
		if !cos.IsNotExist(err, ecode) {
			nlog.Infoln(ftcg, "(range read)", lom.Cname(), err, ecode)
		}
		return ecode, err
	}

	// response headers (compare w/ goi.setwhdr and t.headObjS3)
	whdr.Set(cos.HdrContentType, cos.ContentBinary)
	if goi.dpq.isS3 {
		whdr.Set(cos.HdrContentLength, strconv.FormatInt(hrng.Length, 10))
		s3.SetS3Headers(whdr, lom)
	} else {
		cmn.ToHeader(oa, whdr, hrng.Length)
	}
	goi.w.WriteHeader(http.StatusPartialContent)

	// transmit
	var written int64
	for i := range segs {
		if i > 0 {
			if src, _, err = rc.open(&segs[i]); err != nil {
				break
			}
		}
		var n int64
		n, err = rc.tx(&segs[i], src)
		written += n
		if err != nil {
			break
		}
	}

	if err != nil || written != hrng.Length {
		if !cos.IsErrRetriableConn(err) {
			nlog.Infoln(ftcg, "(range tx)", lom.Cname(), "err:", err, "written:", written, "expected:", hrng.Length)
		}
		return 0, cmn.ErrGetTxBenign // (already committed 206)
	}

	goi.stats(written)
	return 0, nil
}

func (goi *getOI) rcInit(oa *cmn.ObjAttrs) *rcache {
	var (
		lom = goi.lom
		rc  = &rcache{goi: goi, uname: lom.Uname(), size: oa.Size}
	)
	if oa.Size <= 0 || lom.ValidateColdGet() {
		return rc
	}
	id := rcID(oa)
	if id == "" { // cannot tell whether the remote object has changed
		return rc
	}
	u, err := core.NewUfest(id, lom, false /*must-exist*/)
	if err != nil {
		nlog.Warningln(ftcg, "(range cache)", lom.Cname(), "err:", err)
		return rc
	}
	if err := u.LoadPartial(lom); err != nil && !cos.IsNotExist(err) {
		nlog.Warningln(ftcg, "(range cache)", lom.Cname(), "err:", err)
		return rc
	}

	rc.u = u
	rc.chunkSize, rc.total = rcGrid(oa.Size)
	_, loaded := rcFilling.LoadOrStore(rc.uname, struct{}{})
	rc.filling = !loaded
	return rc
}

// persist the accumulated chunks; when the grid is fully covered, complete the
// manifest - the object becomes a regular (chunked) object
func (rc *rcache) fini() {
	if !rc.filling {
		return
	}
	defer rcFilling.Delete(rc.uname)

	lom := rc.goi.lom
	if rc.added {
		if err := rc.u.StorePartial(lom, false /*locked*/); err != nil {
			nlog.Warningln(ftcg, "(range cache)", lom.Cname(), "err:", err)
			return
		}
	}
	if rc.u.Count() != rc.total || rc.u.Size() != rc.size {
		return
	}
	if !lom.UpgradeLock() { // (concurrent reader - next time)
		return
	}
	err := rc.complete()
	lom.DowngradeLock()
	if err != nil {
		nlog.Warningln(ftcg, "(range cache complete)", lom.Cname(), "err:", err)
	}
}

func (rc *rcache) complete() error {
	lom := rc.goi.lom
	if ty := lom.CksumConf().Type; ty != cos.ChecksumNone {
		cksumH := cos.NewCksumHash(ty)
		if err := rc.u.ComputeWholeChecksum(cksumH); err != nil {
			return err
		}
		lom.SetCksum(&cksumH.Cksum)
	}
	return lom.CompleteUfest(rc.u, true /*locked*/)
}

// split the requested range into segments (see rcseg), coalescing
// consecutive not-cached chunks to read them in one shot
func (rc *rcache) plan(hrng *htrange) []rcseg {
	end := hrng.Start + hrng.Length
	if rc.u == nil {
		return []rcseg{{off: hrng.Start, size: hrng.Length}}
	}
	var (
		first = int(hrng.Start / rc.chunkSize)
		last  = int((end - 1) / rc.chunkSize)
		segs  = make([]rcseg, 0, last-first+1)
	)
	for i := first; i <= last; i++ {
		s := rcseg{num: i + 1, coff: int64(i) * rc.chunkSize}
		s.clen = min(rc.chunkSize, rc.size-s.coff)
		s.off = max(hrng.Start, s.coff)
		s.size = min(end, s.coff+s.clen) - s.off

		if s.path = rc.cached(s.num, s.clen); s.path != "" {
			segs = append(segs, s)
			continue
		}
		if l := len(segs); l > 0 && segs[l-1].path == "" {
			prev := &segs[l-1] // extend the run
			prev.clen, prev.size = prev.clen+s.clen, prev.size+s.size
			if rc.filling {
				prev.count++
			}
			continue
		}
		if rc.filling {
			s.count = 1
		} else {
			s.num = 0
		}
		segs = append(segs, s)
	}
	return segs
}

// (returning the pathname: the manifest may get modified in-place, see Ufest._add)
func (rc *rcache) cached(num int, size int64) string {
	c, err := rc.u.GetChunk(num)
	if err != nil || c.Size() != size {
		return ""
	}
	return c.Path()
}

// resolve the segment's byte source
func (rc *rcache) open(s *rcseg) (src rcsrc, ecode int, err error) {
	goi := rc.goi
	if s.path != "" {
		fh, errN := os.Open(s.path)
		if errN == nil {
			return rcsrc{io.NewSectionReader(fh, s.off-s.coff, s.size), fh}, 0, nil
		}
		// gone (e.g., removed by space-cleanup) - fall back to the backend
		nlog.Warningln(ftcg, "(range cache)", s.path, "err:", errN)
		s.path, s.num = "", 0
	}

	off, size := s.off, s.size
	if s.count > 0 {
		off, size = s.coff, s.clen // caching: read the entire (grid-aligned) run
	}
	res := goi.t.Backend(goi.lom.Bck()).GetObjReader(goi.ctx, goi.lom, off, size)
	if res.Err != nil {
		return src, res.ErrCode, res.Err
	}
	goi.rget = true
	return rcsrc{res.R, res.R}, 0, nil
}

// transmit the segment; return the number of bytes transmitted
func (rc *rcache) tx(s *rcseg, src rcsrc) (int64, error) {
	goi := rc.goi
	defer cos.Close(src.cl)

	if s.count > 0 {
		return rc.txStore(s, src)
	}
	buf, slab := goi.t.gmm.AllocSize(_txsize(s.size))
	written, err := cos.CopyBuffer(goi.w, src.r, buf)
	slab.Free(buf)
	return written, err
}

// transmit the requested sub-range of the run and, at the same time, store its chunks
func (rc *rcache) txStore(s *rcseg, src rcsrc) (int64, error) {
	var (
		goi       = rc.goi
		lom       = goi.lom
		cksumType = lom.CksumConf().Type
		sw        = &sectionWriter{w: goi.w, skip: s.off - s.coff, left: s.size}
		buf, slab = goi.t.gmm.AllocSize(_txsize(rc.chunkSize))
	)
	defer slab.Free(buf)

	for i := range s.count {
		var (
			num  = s.num + i
			coff = s.coff + int64(i)*rc.chunkSize
			size = min(rc.chunkSize, rc.size-coff)
		)
		c, err := rc.u.NewChunk(num, lom)
		if err != nil {
			return rc.drain(sw, src, buf, err)
		}
		fh, err := lom.CreatePart(c.Path())
		if err != nil {
			return rc.drain(sw, src, buf, err)
		}

		written, cksum, err := cos.CopyAndChecksum(cos.NewWriterMulti(sw, fh), io.LimitReader(src.r, size),
			buf, cksumType)
		cos.Close(fh)
		if err == nil && written != size {
			err = fmt.Errorf("chunk %d: read %d, expected %d", num, written, size)
		}
		if err != nil {
			if errN := cos.RemoveFile(c.Path()); errN != nil {
				nlog.Errorln(ftcg, "(range cache)", c.Path(), "err:", errN)
			}
			return sw.n, err
		}

		if cksum != nil {
			c.SetCksum(&cksum.Cksum)
		}
		if err := rc.u.Add(c, written, int64(num)); err != nil {
			return rc.drain(sw, src, buf, err) // (keep the chunk: it may be recorded already)
		}
		rc.added = true
	}
	return sw.n, nil
}

// cannot store: transmit whatever is left of the run
func (rc *rcache) drain(sw *sectionWriter, src rcsrc, buf []byte, err error) (int64, error) {
	nlog.Warningln(ftcg, "(range cache)", rc.goi.lom.Cname(), "err:", err)
	_, err = cos.CopyBuffer(sw, src.r, buf)
	return sw.n, err
}

//
// helpers
//

// fixed grid, with the granularity adjusted to fit core.MaxChunkCount
func rcGrid(size int64) (chunkSize int64, total int) {
	chunkSize = rcChunkSize
	if l := cos.DivCeil(size, core.MaxChunkCount); l > chunkSize {
		chunkSize = cos.CeilAlignI64(l, rcChunkSize)
	}
	return chunkSize, int(cos.DivCeil(size, chunkSize))
}

// derive manifest ID from the remote object's version and/or ETag
// (returns "" when there's neither - ie., when there's no way to tell
// whether the remote object has changed)
func rcID(oa *cmn.ObjAttrs) string {
	var (
		verMD, _ = oa.GetCustomKey(cmn.VersionObjMD)
		etag, _  = oa.GetCustomKey(cmn.ETag)
		ver      = oa.Version() // remote ais: own version (a sequence number)
	)
	if verMD == "" && etag == "" && ver == "" {
		return ""
	}
	s := fmt.Sprintf("%s/%s/%s/%d", verMD, etag, ver, oa.Size)
	return fmt.Sprintf("%s%016x", rcIDprefix, onexxh.Checksum64S(cos.UnsafeB(s), cos.MLCG32))
}

// forwards only the [skip, skip+left) sub-range of the stream
type sectionWriter struct {
	w    io.Writer
	skip int64
	left int64
	n    int64 // forwarded
}

func (sw *sectionWriter) Write(p []byte) (int, error) {
	l := len(p)
	if sw.skip > 0 {
		if int64(l) <= sw.skip {
			sw.skip -= int64(l)
			return l, nil
		}
		p, sw.skip = p[sw.skip:], 0
	}
	if int64(len(p)) > sw.left {
		p = p[:sw.left]
	}
	if len(p) == 0 {
		return l, nil
	}
	n, err := sw.w.Write(p)
	sw.left -= int64(n)
	sw.n += int64(n)
	if err != nil {
		return l - len(p) + n, err
	}
	return l, nil
}
