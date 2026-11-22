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
	"time"

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
	entries   cmn.LsoEntries // current chunk payload (empty when only hdr is loaded)
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
		ObjName: nbi.bck.SysObjName(invName),
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
	debug.AssertFunc(func() bool { return nbi.cksum.H.Size() == cos.SizeofI32 })

	nbi.buf, nbi.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)

	// readahead first chunk
	if err := nbi.readChunk(1, ""); err != nil {
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

// set atime to inventory creation time - to specifically satisfy S3 clients
// that require LastModified in every listed object (e.g., `aws s3 ls`)
// - s3.entryToS3 uses atime as LastModified unless S3Props() has the recorded one
// - override atime recorded in the inventory (formatted at creation time, not as RFC3339)
// - note: modifies entries in place (possibly shared with nbi.entries and nbi.cache)
func (nbi *nbiCtx) fillAtime(msg *apc.LsoMsg, entries cmn.LsoEntries) {
	if !msg.IsFlagSet(apc.LsIsS3) || len(entries) == 0 {
		return
	}
	atime := nbi.ufest.Created().UTC().Format(time.RFC3339)
	for _, en := range entries {
		if !en.IsAnyFlagSet(apc.EntryIsDir) {
			en.Atime = atime
		}
	}
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

// install the header and drop all entries from the previous chunk
func (nbi *nbiCtx) setHdr(num int, hdr nbiChunkHdr) {
	nbi.chunkNum, nbi.hdr, nbi.nidx = num, hdr, 0
	clear(nbi.entries)
	nbi.entries = nbi.entries[:0]
}

func (nbi *nbiCtx) readChunk(num int, token string) error {
	var hdr nbiChunkHdr
	fh, err := nbi.openChunk(num, &hdr)
	if err != nil {
		return err
	}
	defer _closeChunk(fh)
	nbi.setHdr(num, hdr)
	if hdr.last <= token { // skip decoding payload if the chunk ends at or before `token`
		return nil
	}
	return nbi.readPayload(fh)
}

func _closeChunk(fh *os.File) {
	if fh != nil {
		cos.Close(fh)
	}
}

// read the header and leave the file positioned at the payload (nil file on cache hit)
func (nbi *nbiCtx) openChunk(num int, hdr *nbiChunkHdr) (*os.File, error) {
	if num == nbi.cache.chunkNum {
		*hdr = nbi.cache.hdr
		return nil, nil
	}
	chunk, err := nbi.ufest.GetChunk(num)
	if err != nil {
		return nil, nbi.emit(err)
	}
	fh, err := os.Open(chunk.Path())
	if err != nil {
		return nil, nbi.emit(err)
	}
	if err := nbi.readHdr(fh, num, hdr); err != nil {
		cos.Close(fh)
		return nil, err
	}
	return fh, nil
}

func (nbi *nbiCtx) readHdr(fh *os.File, num int, hdr *nbiChunkHdr) error {
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
	debug.AssertFunc(func() bool { return nbi.cksum != nil && nbi.cksum.Ty() == cos.ChecksumCRC32C }, "must have CRC32c")
	nbi.cksum.H.Reset()
	nbi.cksum.H.Write(hdrBuf)
	crc := nbi.cksum.SumTo()
	exp := binary.BigEndian.Uint32(frame[cos.SizeofI32:])
	got := binary.BigEndian.Uint32(crc)
	if got != exp {
		return nbi.emit(fmt.Errorf("invalid chunk [%d, %q] header hash: expected %08x, got %08x",
			num, fh.Name(), exp, got))
	}

	if err := hdr.Unpack(cos.NewUnpacker(hdrBuf)); err != nil {
		return nbi.emit(err)
	}
	return nil
}

// nil fh: copy entries from the cache (see openChunk)
func (nbi *nbiCtx) readPayload(fh *os.File) error {
	if fh == nil {
		debug.Assert(nbi.chunkNum == nbi.cache.chunkNum)
		nbi.entries = append(nbi.entries, nbi.cache.entries...)
		return nil
	}
	// 3. read chunk payload: msgp entries (note: hdrBuf consumed before msgp reuses the buffer)
	mr := msgp.NewReaderBuf(fh, nbi.buf)
	if cnt := int(nbi.hdr.entryCount); cap(nbi.entries) < cnt {
		nbi.entries = make(cmn.LsoEntries, 0, cnt)
	}
	if err := nbi.entries.DecodeMsg(mr); err != nil {
		// discard partial entries so a retry reloads the payload
		clear(nbi.entries)
		nbi.entries = nbi.entries[:0]
		return nbi.emit(err)
	}

	// TODO: remove eventually
	debug.Assert(len(nbi.entries) == int(nbi.hdr.entryCount))
	debug.Assert(len(nbi.entries) > 0)
	debug.AssertFunc(func() bool { return nbi.hdr.first == nbi.entries[0].Name })
	debug.AssertFunc(func() bool { return nbi.hdr.last == nbi.entries[len(nbi.entries)-1].Name })

	return nil
}

func (nbi *nbiCtx) skip() { nbi.nidx = len(nbi.entries) } // in re: prefix

// nrCollapse checks whether the entry should be collapsed into a virtual directory
// (i.e., the object name has a '/' separator below the listing prefix).
// If so, appends the directory entry to lst (when past the continuation token and start-after)
// and returns the directory prefix for subsequent skipping.
// Returns "" when no collapsing is needed.
func nrCollapse(name string, msg *apc.LsoMsg, lst *cmn.LsoRes) string {
	rel := strings.TrimPrefix(name, msg.Prefix)
	idx := strings.Index(rel, cos.PathSeparator)
	if idx < 0 {
		// no subdirectory, no collapsing needed
		return ""
	}
	dirName := msg.Prefix + rel[:idx+1]
	if dirName > max(msg.ContinuationToken, msg.StartAfter) {
		lst.Entries = append(lst.Entries, &cmn.LsoEnt{Name: dirName, Flags: apc.EntryIsDir})
	}
	return dirName
}

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

	// set again only if this page returns a token
	prev := nbi.prevToken
	nbi.prevToken = ""

	token := max(msg.ContinuationToken, msg.StartAfter)
	switch token {
	case "":
		if nbi.chunkNum != 1 || len(nbi.entries) == 0 {
			if err := nbi.readChunk(1, ""); err != nil {
				return err
			}
		}
		nbi.nidx = 0
	case prev:
		// do nothing
	default:
		if err := nbi.seekAfter(token); err != nil {
			return err
		}
	}

	pageSize := nbi.pageSize(msg) // apply msg.PageSize

	noRecursion := msg.IsFlagSet(apc.LsNoRecursion)
	var skipDir string // when non-empty, skip entries under this virtual directory

	for len(lst.Entries) < pageSize {
		// next chunk
		if nbi.nidx >= len(nbi.entries) {
			if nbi.chunkNum == nbi.ufest.Count() {
				return nil
			}
			// clone entries from the current (soon previous)  chunk
			for i, e := range lst.Entries {
				cp := *e
				lst.Entries[i] = &cp
			}
			nbi.cacheIt()
			if err := nbi.readChunk(nbi.chunkNum+1, ""); err != nil {
				return err
			}
		}
		debug.AssertFunc(func() bool {
			return nbi.nidx == 0 || nbi.nidx == len(nbi.entries) || nbi.entries[nbi.nidx].Name > nbi.entries[nbi.nidx-1].Name
		})
		debug.AssertFunc(func() bool {
			return nbi.nidx == 0 || nbi.nidx == len(nbi.entries) || nbi.entries[nbi.nidx].Name > nbi.hdr.first
		})

		// non-recursive: skip entries under a previously collapsed virtual directory
		if skipDir != "" {
			entries := nbi.entries[nbi.nidx:]
			i := sort.Search(len(entries), func(i int) bool {
				return !strings.HasPrefix(entries[i].Name, skipDir)
			})
			nbi.nidx += i
			if i >= len(entries) {
				continue
			}
			skipDir = "" // reset when the current entry no longer falls under the collapsed directory
		}

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
				break // past the prefix range
			}
			nbi.nidx++
			if noRecursion {
				if skipDir = nrCollapse(out.Name, msg, lst); skipDir != "" {
					continue
				}
			}
			lst.Entries = append(lst.Entries, out)
			continue
		}
		out := nbi.entries[nbi.nidx]
		nbi.nidx++ // next
		if noRecursion {
			if skipDir = nrCollapse(out.Name, msg, lst); skipDir != "" {
				continue
			}
		}
		lst.Entries = append(lst.Entries, out)
	}

	if l := len(lst.Entries); l > 0 {
		lst.ContinuationToken = lst.Entries[l-1].Name
		debug.Assert(lst.ContinuationToken != "")

		// eof
		if nbi.nidx >= len(nbi.entries) && nbi.chunkNum == nbi.ufest.Count() {
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

// walk left (<---) by header, retaining the current chunk when the token falls in a gap
func (nbi *nbiCtx) rewind(token string) error {
	debug.Assert(nbi.chunkNum > 1)
	for num := nbi.chunkNum - 1; num > 0; num-- {
		var hdr nbiChunkHdr
		fh, err := nbi.openChunk(num, &hdr)
		if err != nil {
			return err
		}
		if hdr.last > token {
			nbi.setHdr(num, hdr)
			if num > 1 && token < hdr.first {
				_closeChunk(fh)
				continue
			}
			err = nbi.readPayload(fh)
		}
		_closeChunk(fh)
		return err
	}
	return nil
}

func (nbi *nbiCtx) seekAfter(token string) error {
	debug.Assert(token != "")
	if nbi.chunkNum > 1 && token < nbi.hdr.first {
		if err := nbi.rewind(token); err != nil {
			return err
		}
	}
	for token >= nbi.hdr.last {
		if nbi.chunkNum == nbi.ufest.Count() {
			nbi.skip()
			return nil
		}
		if len(nbi.entries) > 0 {
			nbi.cacheIt()
		}
		if err := nbi.readChunk(nbi.chunkNum+1, token); err != nil {
			return err
		}
	}
	if len(nbi.entries) == 0 {
		if err := nbi.readChunk(nbi.chunkNum, ""); err != nil {
			return err
		}
	}
	nbi.nidx = sort.Search(len(nbi.entries), func(i int) bool { return nbi.entries[i].Name > token })
	debug.Assert(nbi.nidx < len(nbi.entries))
	return nil
}
