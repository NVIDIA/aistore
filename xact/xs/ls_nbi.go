// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/tinylib/msgp/msgp"
)

// NBI pagination note:
//
// In the context of inventory listing, lsmsg.PageSize is best-effort and approximate:
// - each target delivers an approximate share of the requested page size,
//   subject to local chunking, minimum bounds, and slight overfetch;
// - proxy then merges, sorts, and truncates the combined result.
// - the actual number of returned entries may differ from the requested PageSize
//   (if specified).
// PageSize=0 is strongly recommended: lets the system optimize per-target
// chunk reads and minimize the total number of distributed-merge roundtrips.

const (
	minPageSize = max(10, apc.MinInvNamesPerChunk) // 10
)

// TODO:
// - feat: support LsCached (semantics? a) per recorded in chunks or b) w/ respect to local data)
// - feat: support listing jobs that currently rely-on/reuse:
//   - npgCtx.nextPageR() (bucket summary)
//   - npgCtx.nextPageA() (all list-range jobs: prefetch, evict, transform-copy-objs, etc.)
// - returned errors: unify; add details

type nbiCtx struct {
	lom       *core.LOM      // .sys_inventory/BUCKET-UNAME/INV-NAME
	ufest     *core.Ufest    // completed and loaded
	cksum     *cos.CksumHash // chunk header checksum (protection)
	slab      *memsys.Slab   // see (reusable buffer)
	bck       *meta.Bck      // source bucket
	prevToken string         // responded with prev. nextPage() call
	hdr       nbiChunkHdr    // current chunk header
	entries   cmn.LsoEntries // decoded from the current chunk
	buf       []byte         // reusable buffer: chunk meta, msgpack
	cache     struct {
		hdr      nbiChunkHdr
		entries  cmn.LsoEntries
		chunkNum int
	}
	nidx     int // next index within `entries` (limited scope)
	chunkNum int // current chunk number (1-based)
	nat      int // number of active targets (excepting mainternance mode)
}

func (nbi *nbiCtx) init(invName string) error {
	lom := &core.LOM{
		ObjName: nbiObjName(nbi.bck, invName),
	}
	if err := lom.InitBck(meta.SysBckNBI()); err != nil {
		return err
	}

	lom.Lock(false) // r-lock for the listing duration

	if err := nbi._load(lom); err != nil {
		nbi.cleanup()
		return err
	}
	if nbi.ufest.Count() == 0 {
		// see "special" below and create_nbi; cleanup() checks buf != nil, etc.
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(core.T.String(), "- empty inventory", invName, lom.Cname())
		}
		return nil
	}

	nbi.cksum = cos.NewCksumHash(cos.ChecksumCRC32C)
	debug.Assert(nbi.cksum.H.Size() == cos.SizeofI32)

	nbi.buf, nbi.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)
	nbi.nidx = 0

	// readahead first chunk
	nbi.chunkNum = 1
	if err := nbi.readChunk(); err != nil {
		nbi.cleanup()
		return err
	}

	smap := core.T.Sowner().Get()
	nbi.nat = smap.CountActiveTs()

	if nbi.nat == 0 {
		nbi.cleanup()
		return cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
	}

	hroom := max(nbi.hdr.entryCount>>4, 128)
	nbi.cache.entries = make(cmn.LsoEntries, 0, nbi.hdr.entryCount+hroom)

	return nil
}

func (nbi *nbiCtx) _load(lom *core.LOM) error {
	if err := lom.Load(true, true); err != nil {
		return err
	}

	// special: when bucket is "smaller" than cluster
	if lom.Lsize() == 0 {
		nbi.lom = lom
		nbi.ufest = &core.Ufest{}
		return nil
	}

	ufest, err := core.NewUfest("", lom, true /*mustExist*/)
	if err != nil {
		return nbi.emit(err)
	}
	if err := ufest.LoadCompleted(lom); err != nil {
		return nbi.emit(err)
	}

	nbi.lom = lom
	nbi.ufest = ufest
	return nil
}

func (nbi *nbiCtx) emit(err error) error {
	e := fmt.Errorf("native bucket inventory: %w [%s]", err, nbi.bck.Cname(""))
	if cmn.Rom.V(4, cos.ModXs) {
		nlog.Errorln(e)
	}
	return e
}

func (nbi *nbiCtx) cleanup() {
	if nbi.lom != nil {
		nbi.lom.Unlock(false)
	}
	if nbi.buf != nil && nbi.slab != nil {
		nbi.slab.Free(nbi.buf)
	}
	clear(nbi.entries)
	nbi.entries = nbi.entries[:0]
	clear(nbi.cache.entries)
	nbi.cache.entries = nbi.cache.entries[:0]
	nbi.cache.chunkNum = 0
	nbi.cache.hdr = nbiChunkHdr{}
	nbi.prevToken = ""
}

func (nbi *nbiCtx) cacheIt() {
	if nbi.chunkNum <= 0 || len(nbi.entries) == 0 {
		debug.Assert(false, "invalid state: ", nbi.chunkNum, " len: ", len(nbi.entries))
		return
	}
	if nbi.chunkNum >= nbi.ufest.Count() {
		return // not caching last (proxy's minToken can only rewind)
	}
	if nbi.cache.chunkNum == nbi.chunkNum {
		return // already cached
	}

	clear(nbi.cache.entries)
	cnt := len(nbi.entries)
	if cap(nbi.cache.entries) < cnt {
		nbi.cache.entries = make(cmn.LsoEntries, cnt)
	} else {
		nbi.cache.entries = nbi.cache.entries[:cnt]
	}
	copy(nbi.cache.entries, nbi.entries)
	nbi.cache.hdr = nbi.hdr
	nbi.cache.chunkNum = nbi.chunkNum
}

func (nbi *nbiCtx) readChunk() error {
	// cache
	if nbi.chunkNum == nbi.cache.chunkNum {
		clear(nbi.entries)
		cnt := len(nbi.cache.entries)
		if cap(nbi.entries) < cnt {
			nbi.entries = make(cmn.LsoEntries, cnt)
		} else {
			nbi.entries = nbi.entries[:cnt]
		}
		copy(nbi.entries, nbi.cache.entries)
		nbi.hdr = nbi.cache.hdr
		return nil
	}

	chunk, err := nbi.ufest.GetChunk(nbi.chunkNum)
	if err != nil {
		return nbi.emit(err)
	}

	fh, err := os.Open(chunk.Path())
	if err != nil {
		return nbi.emit(err)
	}
	defer cos.Close(fh)

	// 1. read chunk header length
	var frame [nbiFrameSize]byte
	if _, err := io.ReadFull(fh, frame[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nbi.emit(err)
	}

	// 2. read chunk header
	hdrLen := binary.BigEndian.Uint32(frame[:])
	if hdrLen == 0 || hdrLen > nbiMaxHdrLen {
		return nbi.emit(fmt.Errorf("invalid chunk header length %d", hdrLen))
	}

	hdrBuf := nbi.buf[:hdrLen] // note: using the same buffer for hdr and msgp payload
	if _, err := io.ReadFull(fh, hdrBuf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nbi.emit(err)
	}

	// 2.1. checksum
	debug.Assert(nbi.cksum != nil && nbi.cksum.Ty() == cos.ChecksumCRC32C, "must have CRC32c")
	nbi.cksum.H.Reset()
	nbi.cksum.H.Write(hdrBuf)
	crc := nbi.cksum.SumTo()
	exp := binary.BigEndian.Uint32(frame[cos.SizeofI32:])
	got := binary.BigEndian.Uint32(crc)
	if got != exp {
		return nbi.emit(fmt.Errorf("invalid chunk [%d, %q] header hash: expected %08x, got %08x",
			nbi.chunkNum, chunk.Path(), exp, got))
	}

	unpacker := cos.NewUnpacker(hdrBuf)
	if err := nbi.hdr.Unpack(unpacker); err != nil {
		return nbi.emit(err)
	}

	nbi.nidx = 0

	// 3. read chunk payload: msgp entries (note: hdrBuf consumed before msgp reuses the buffer)
	mr := msgp.NewReaderBuf(fh, nbi.buf)

	if cnt := int(nbi.hdr.entryCount); cap(nbi.entries) < cnt {
		nbi.entries = make(cmn.LsoEntries, 0, cnt)
	} else {
		nbi.entries = nbi.entries[:0]
	}
	if err := nbi.entries.DecodeMsg(mr); err != nil {
		return err
	}

	// TODO: remove eventually
	debug.Assert(len(nbi.entries) == int(nbi.hdr.entryCount))
	debug.Assert(len(nbi.entries) > 0)
	debug.Assert(nbi.hdr.first == nbi.entries[0].Name)
	debug.Assert(nbi.hdr.last == nbi.entries[len(nbi.entries)-1].Name)

	return nil
}

func (nbi *nbiCtx) skip() { nbi.nidx = len(nbi.entries) } // in re: prefix

func (nbi *nbiCtx) pageSize(msg *apc.LsoMsg) int {
	// chunk-driven (optimize local reads/merge roundtrips)
	n := max(len(nbi.entries), minPageSize)
	if msg.PageSize == 0 {
		return n
	}

	// honor requested size approximately, per target
	share := cos.DivRound(int(msg.PageSize), nbi.nat)
	return max(minPageSize, cos.DivRound(share*5, 4)) // overshoot by 25%
}

func (nbi *nbiCtx) nextPage(msg *apc.LsoMsg, lst *cmn.LsoRes) error {
	lst.Entries = lst.Entries[:0]
	lst.ContinuationToken = ""

	if nbi.ufest.Count() == 0 {
		return nil
	}

	switch msg.ContinuationToken {
	case "":
		debug.Assert(nbi.chunkNum == 1 && nbi.nidx == 0)
	case nbi.prevToken:
		// do nothing
	default:
		// resync nbi iterator
		nbi.nidx = 0
		if msg.ContinuationToken < nbi.hdr.first {
			if err := nbi.rewind(msg.ContinuationToken); err != nil {
				return err
			}
		} else if nbi.chunkNum > nbi.ufest.Count() {
			if msg.ContinuationToken >= nbi.hdr.last {
				// we are done
				return nil
			}
			if err := nbi.rewind(msg.ContinuationToken); err != nil {
				return err
			}
		}
		if err := nbi.seekAfter(msg.ContinuationToken); err != nil {
			return err
		}
		if nbi.chunkNum > nbi.ufest.Count() {
			// no more chunks - we are done
			return nil
		}
	}

	pageSize := nbi.pageSize(msg) // apply msg.PageSize

	for len(lst.Entries) < pageSize {
		// next chunk
		if nbi.nidx >= len(nbi.entries) {
			// clone entries from the current (soon previous)  chunk
			for i, e := range lst.Entries {
				cp := *e
				lst.Entries[i] = &cp
			}
			nbi.cacheIt()
			nbi.chunkNum++
			if nbi.chunkNum > nbi.ufest.Count() {
				// no more chunks - we are done
				return nil
			}
			if err := nbi.readChunk(); err != nil {
				return err
			}
		}
		debug.Assert(nbi.nidx == 0 || nbi.nidx == len(nbi.entries) || nbi.entries[nbi.nidx].Name > nbi.entries[nbi.nidx-1].Name)
		debug.Assert(nbi.nidx == 0 || nbi.nidx == len(nbi.entries) || nbi.entries[nbi.nidx].Name > nbi.hdr.first)

		// prefix
		if msg.Prefix != "" {
			if nbi.nidx < len(nbi.entries) && nbi.entries[nbi.nidx].Name < msg.Prefix {
				entries := nbi.entries[nbi.nidx:]
				i := sort.Search(len(entries), func(i int) bool {
					return entries[i].Name >= msg.Prefix
				})
				if i >= len(entries) {
					nbi.skip()
					continue
				}
				nbi.nidx += i
			}

			out := nbi.entries[nbi.nidx]
			if !strings.HasPrefix(out.Name, msg.Prefix) {
				nbi.nidx++
				continue
			}
			nbi.nidx++
			lst.Entries = append(lst.Entries, out)
			continue
		}
		out := nbi.entries[nbi.nidx]
		nbi.nidx++ // next
		lst.Entries = append(lst.Entries, out)
	}

	if l := len(lst.Entries); l > 0 {
		lst.ContinuationToken = lst.Entries[l-1].Name
		debug.Assert(lst.ContinuationToken != "")

		// eof
		if nbi.nidx >= len(nbi.entries) && nbi.chunkNum > nbi.ufest.Count() {
			lst.ContinuationToken = ""
		} else if msg.Prefix != "" {
			// out of prefix-defined range
			if nbi.nidx < len(nbi.entries) && !strings.HasPrefix(nbi.entries[nbi.nidx].Name, msg.Prefix) {
				lst.ContinuationToken = ""
			}
		}
	}

	nbi.prevToken = lst.ContinuationToken

	return nil
}

//
// cont. token helpers
//

func (nbi *nbiCtx) seekAfter(token string) error {
	debug.Assert(token != "")
	debug.Assert(nbi.nidx == 0) // otherwise we better search from nidx onwards

	// fast path: search current chunk
	if nbi.nidx < len(nbi.entries) && len(nbi.entries) > 0 && token < nbi.hdr.last {
		i := sort.Search(len(nbi.entries), func(i int) bool {
			return nbi.entries[i].Name > token
		})
		if i < len(nbi.entries) {
			nbi.nidx = i
			debug.Assert(nbi.entries[i].Name > token, "seekAfter: ", token, " -> ", nbi.entries[i].Name)
			return nil
		}
	}

	// token is >= current chunk's last item, move forward until we find
	// the first chunk whose range may contain something > token
	for {
		nbi.cacheIt()
		nbi.chunkNum++
		if nbi.chunkNum > nbi.ufest.Count() {
			return nil
		}
		if err := nbi.readChunk(); err != nil {
			return err
		}
		if len(nbi.entries) == 0 || nbi.hdr.last <= token {
			continue
		}
		i := sort.Search(len(nbi.entries), func(i int) bool { return nbi.entries[i].Name > token })
		if i < len(nbi.entries) {
			nbi.nidx = i
			debug.Assert(nbi.entries[i].Name > token, "seekAfter: ", token, " -> ", nbi.entries[i].Name)
			return nil
		}
	}
}

func (nbi *nbiCtx) rewind(token string) error {
	debug.Assert(nbi.chunkNum <= nbi.ufest.Count()+1, nbi.chunkNum, " vs ", nbi.ufest.Count())
	nbi.chunkNum = min(nbi.chunkNum, nbi.ufest.Count())
	for nbi.chunkNum > 1 {
		nbi.chunkNum--
		if err := nbi.readChunk(); err != nil {
			return err
		}
		if token >= nbi.hdr.first {
			nbi.nidx = 0
			return nil
		}
	}
	nbi.nidx = 0
	// reached all the way to the left boundary with continuation token
	// pointing even more to the left (which is legal)
	return nil
}
