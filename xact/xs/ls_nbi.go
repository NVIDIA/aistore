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

// TODO -- FIXME:
// - corner case: very small inventory w/ not every target having chunks
// - test list-objects with empty invName (target must resolve single)
// - feat: support LsCached (semantics? a) per recorded in chunks or b) w/ respect to local data)
// - feat: support listing jobs that currently rely-on/reuse:
//   - npgCtx.nextPageR() (bucket summary)
//   - npgCtx.nextPageA() (all list-range jobs: prefetch, evict, transform-copy-objs, etc.)
// - returned errors: unify; add details
// - pagesize default set by proxy (10k when zero) - not optimal for NBI

type nbiCtx struct {
	bck   *meta.Bck
	lom   *core.LOM   // .sys_inventory/BUCKET-UNAME/INV-NAME
	ufest *core.Ufest // completed and loaded

	// runtime state
	chunkNum  int            // current chunk number (1-based)
	nidx      int            // next index within LsoEntries (limited scope)
	entries   cmn.LsoEntries // decoded from current chunk
	hdr       nbiChunkHdr    // current chunk header=[first, last, cnt]
	prevToken string         // responded

	buf   []byte
	slab  *memsys.Slab
	cksum *cos.CksumHash
}

func (nbi *nbiCtx) init(invName string) error {
	lom := core.AllocLOM(nbiObjName(nbi.bck, invName))
	if err := lom.InitBck(meta.SysBckNBI()); err != nil {
		core.FreeLOM(lom)
		return err
	}

	lom.Lock(false) // r-lock for the listing duration

	if err := nbi._load(lom); err != nil {
		nbi.cleanup()
		return err
	}

	nbi.cksum = cos.NewCksumHash(cos.ChecksumCRC32C)
	debug.Assert(nbi.cksum.H.Size() == cos.SizeofI32)

	nbi.buf, nbi.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)
	nbi.nidx = 0
	nbi.chunkNum = 1
	if err := nbi.readChunk(); err != nil {
		nbi.cleanup()
		return err
	}

	return nil
}

func (nbi *nbiCtx) _load(lom *core.LOM) error {
	if err := lom.Load(true, true); err != nil {
		return err
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
		core.FreeLOM(nbi.lom)
	}
	if nbi.buf != nil && nbi.slab != nil {
		nbi.slab.Free(nbi.buf)
	}
}

func (nbi *nbiCtx) readChunk() error {
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

	// TODO: remove
	debug.Assert(len(nbi.entries) == int(nbi.hdr.entryCount))
	debug.Assert(len(nbi.entries) > 0)
	debug.Assert(nbi.hdr.first == nbi.entries[0].Name)
	debug.Assert(nbi.hdr.last == nbi.entries[len(nbi.entries)-1].Name)
	return nil
}

func (nbi *nbiCtx) skip() { nbi.nidx = len(nbi.entries) } // in re: prefix

func (nbi *nbiCtx) nextPage(msg *apc.LsoMsg, lst *cmn.LsoRes) error {
	lst.Entries = lst.Entries[:0]
	lst.ContinuationToken = ""

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

	pageSize := cos.Ternary(msg.PageSize <= 0, apc.MaxPageSizeAIS, msg.PageSize)
	for len(lst.Entries) < int(pageSize) {
		// next chunk
		if nbi.nidx >= len(nbi.entries) {
			// clone entries from the current (soon previous)  chunk
			for i, e := range lst.Entries {
				cp := *e
				lst.Entries[i] = &cp
			}

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
