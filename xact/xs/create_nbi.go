// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	"github.com/tinylib/msgp/msgp"
)

// XactNBI: create native bucket inventory (NBI)
//
// TODO in 4.4+ =================================================
// - progress notif
// - designated-target + filterAddLmeta()
// - stats: internal (-> CtlMsg) and Prometheus
// - single backend.ListObjects(0 in the cluster; filterKeepMine
// - multiple inventories: a) periodic re-sync, b) cleanup/GC old
// - disallow user PUT or copy => system buckets (not even admin)
// ==============================================================

// on-disk formatting
const (
	nbiMetaVer   uint8 = 1
	nbiFrameSize       = 2 * cos.SizeofI32 // framing: [length(header) | CRC32c(header) | header ]
	nbiMaxHdrLen       = 4 * cos.KiB       // (unlikely to ever exceed)
)

type (
	nbiChunkHdr struct {
		first      string
		last       string
		entryCount uint32
	}
)

// x-nbi
type (
	nbiFactory struct {
		xctn *XactNBI
		xreg.RenewBase
	}
	XactNBI struct {
		msg    *apc.CreateNBIMsg
		lom    *core.LOM
		ufest  *core.Ufest
		slab   *memsys.Slab
		cksum  *cos.CksumHash
		ctlmsg string
		buf    []byte
		xact.Base
		clean atomic.Bool // true: wi.cleanup() done _or_ recycled via mem-pool
	}
)

// interface guard
var (
	_ core.Xact      = (*XactNBI)(nil)
	_ xreg.Renewable = (*nbiFactory)(nil)
)

func (*nbiFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &nbiFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
}

func (p *nbiFactory) Start() error {
	bck := p.Bucket()
	msg := p.Args.Custom.(*apc.CreateNBIMsg)
	r := &XactNBI{msg: msg}
	r.InitBase(p.UUID(), p.Kind(), bck)

	// inv. name for a given bucket
	invName := r.msg.Name
	debug.Assert(invName != "")

	// nbi LOM is (almost) always chunked
	r.lom = &core.LOM{
		ObjName: r.Bck().SysObjName(invName),
	}
	if err := r.init(); err != nil {
		return err
	}

	_ = r.CtlMsg()
	p.xctn = r
	return nil
}

func (*nbiFactory) Kind() string     { return apc.ActCreateNBI }
func (p *nbiFactory) Get() core.Xact { return p.xctn }

func (p *nbiFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.UUID() != prevEntry.UUID() {
		return wpr, cmn.NewErrXactUsePrev(prevEntry.Get().String())
	}
	bckEq := prevEntry.Bucket().Equal(p.Bucket(), true /*same BID*/, true)
	debug.Assert(bckEq)
	return xreg.WprUse, nil
}

/////////////
// XactNBI //
/////////////

func (r *XactNBI) init() error {
	err := r.lom.InitBck(meta.SysBckNBI())
	if err != nil {
		return err
	}
	uploadID := xact.PrefixInvID + cos.GenUUID()
	r.ufest, err = core.NewUfest(uploadID, r.lom, false /*must-exist*/)
	if err != nil {
		return err
	}
	r.buf, r.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)

	r.cksum = cos.NewCksumHash(cos.ChecksumCRC32C)
	debug.Assert(r.cksum.H.Size() == cos.SizeofI32)

	return nil
}

func (r *XactNBI) Abort(err error) bool {
	if !r.Base.Abort(err) {
		return false
	}
	r.cleanup()
	return true
}

func (r *XactNBI) cleanup() {
	if !r.clean.CAS(false, true) {
		return
	}
	r.slab.Free(r.buf)

	if r.IsAborted() {
		// cleanup partial
		r.ufest.Abort(r.lom)
	}
}

func (r *XactNBI) CtlMsg() string {
	if r.ctlmsg != "" {
		return r.ctlmsg
	}
	sb := &cos.SB{}
	sb.Init(80)
	r.msg.Str(r.Bck().Cname(""), sb)
	r.ctlmsg = sb.String()
	return r.ctlmsg
}

func (r *XactNBI) Snap() (snap *core.Snap) {
	return r.Base.NewSnap(r)
}

// main method
func (r *XactNBI) Run(wg *sync.WaitGroup) {
	wg.Done()

	nlog.Infoln(core.T.String(), "run:", r.Name(), "ctl:", r.ctlmsg)

	bck := r.Bck()
	lsmsg := &r.msg.LsoMsg
	if pgsize := bck.MaxPageSize(); lsmsg.PageSize <= 0 || lsmsg.PageSize > pgsize {
		lsmsg.PageSize = pgsize
	}
	lsmsg.ContinuationToken = ""

	var (
		ntotal    int64
		smap      = core.T.Sowner().Get()
		bp        = core.T.Backend(bck)
		ubuf      = bck.MakeUname("", true)
		lastToken = "__dummy__"
		all       = make(cmn.LsoEntries, 0, r.msg.NamesPerChunk) // prealloc and reuse
		fast      = true
		nonRecurs = lsmsg.IsFlagSet(apc.LsNoRecursion)
	)
	const warn = "backend returned non-reused entries slice; falling back to append path"
	for num := 1; !r.IsAborted() && lastToken != ""; num++ {
		var (
			idx int
		)
		all = all[:0]

		for lastToken != "" && idx < int(r.msg.NamesPerChunk) {
			lst := &cmn.LsoRes{}
			dst := all[idx:idx:cap(all)] // safe for append
			lst.Entries = dst

			if _, err := bp.ListObjects(bck, lsmsg, lst); err != nil {
				r.Abort(err)
				return
			}
			if err := r.filterKeepMine(lst, ubuf, smap); err != nil {
				r.Abort(err)
				return
			}

			// make an exception for apc.LsNoRecursion;
			// otherwise rely on sorted backend.ListObjects()
			if nonRecurs {
				sort.Slice(lst.Entries, func(i, j int) bool {
					return lst.Entries[i].Name < lst.Entries[j].Name
				})
			}

			// backend must reuse the passed-in slice ([:0] + append)
			reused := _sameBacking(dst, lst.Entries)

			switch {
			case fast && !reused: // switch fast => slow
				fast = false
				nlog.Warningln(r.Name(), "fast->slow:", warn) // TODO: find out
				tmp := make(cmn.LsoEntries, 0, r.msg.NamesPerChunk)
				tmp = append(tmp, all[:idx]...)
				tmp = append(tmp, lst.Entries...)
				all = tmp
				idx = len(all)
			case fast: // fast path
				idx += len(lst.Entries)
				all = all[:idx:cap(all)]
			default: // slow path
				all = append(all, lst.Entries...)
				idx = len(all)
			}

			// must make forward progress
			debug.Assert(lst.ContinuationToken == "" || lst.ContinuationToken != lsmsg.ContinuationToken, assertContProgress)

			lastToken = lst.ContinuationToken
			lsmsg.ContinuationToken = lastToken
		}
		if idx == 0 {
			break
		}

		// next chunk
		all = all[:idx]
		ntotal += int64(idx)
		r.ObjsAdd(idx, 0) // like x-lso: count names, zero bytes

		if err := r.writeChunk(num, all); err != nil {
			r.Abort(err)
			return
		}
		if cmn.Rom.V(5, cos.ModXs) {
			if idx == 1 {
				nlog.Warningln(core.T.String(), r.Name(), "single-entry chunk-num:", num, "total:", ntotal)
			} else {
				nlog.Infoln(r.Name(), "chunk-num:", num, "lso-entries:", idx, "total:", ntotal)
			}
		}
	}

	if err := r.fini(smap, ntotal); err != nil {
		r.Abort(err)
	}
}

func (r *XactNBI) fini(smap *meta.Smap, ntotal int64) error {
	var (
		size int64
		lom  = r.lom
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	errLoad := lom.Load(false, true)
	if errLoad == nil {
		// unlikely (and currently impossible given single-inventory-per-bucket)
		return fmt.Errorf("%s: inventory %q already exists (%s)", r.Name(), r.msg.Name, lom.Cname())
	}
	if !cos.IsNotExist(errLoad) {
		return errLoad // IO err
	}

	if r.IsAborted() {
		return nil
	}

	now := time.Now()
	if r.ufest.Count() != 0 {
		err := lom.CompleteUfest(r.ufest, true /*locked*/)
		if err != nil {
			return err
		}
		size = lom.Lsize()
	} else {
		// special: when bucket is "smaller" than the cluster
		debug.Assert(ntotal == 0)
		fh, err := lom.Create()
		if err != nil {
			core.T.FSHC(err, lom.Mountpath(), lom.FQN)
			return err
		}
		cos.Close(fh)
		lom.SetSize(0)
		lom.SetCksum(cos.NoneCksum)
		lom.SetAtimeUnix(now.UnixNano())
		if err := lom.PersistMain(false /*chunked*/); err != nil {
			return err
		}
	}

	// write NBI's own meta
	meta := &apc.NBIMeta{
		Prefix:   r.msg.Prefix,
		Started:  r.StartTime().UnixNano(),
		Finished: now.UnixNano(),
		Ntotal:   ntotal,
		SmapVer:  smap.Version,
		Chunks:   int32(r.ufest.Count()),
		Nat:      int32(smap.CountActiveTs()),
	}
	if err := fs.SetNBI(lom.FQN, meta, r.buf); err != nil {
		nlog.Errorf("%s: ex-post-facto failure to store metadata: [%q, %q, %v]", r.Name(), r.msg.Name, lom.Cname(), err)
		core.T.FSHC(err, lom.Mountpath(), lom.FQN)
		// unlikely; keeping it for possible further troubleshooting
	}

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infof("completed %s [%s]: size %d, chunks %d, lso-entries %d (per chunk: %d)",
			lom.Cname(), r.msg.Name, size, r.ufest.Count(), ntotal, r.msg.NamesPerChunk)
	}

	r.cleanup()
	r.Finish()
	return nil
}

// TODO: ref
func (*XactNBI) filterKeepMine(lst *cmn.LsoRes, ubuf []byte, smap *meta.Smap) error {
	j := 0
	sid := core.T.SID()

	for _, en := range lst.Entries {
		uname := append(ubuf, en.Name...) //nolint:gocritic // reusing ubuf - intentionally not assigning
		si, err := smap.HrwName2T(uname)
		if err != nil {
			return err
		}
		if si.ID() != sid {
			continue
		}

		// NOTE:
		// - always skip virtual dirs (a.k.a. common prefixes)
		// - see also "trailing slash" convention across codebase
		if en.IsAnyFlagSet(apc.EntryIsDir) || cos.IsLastB(en.Name, filepath.Separator) {
			continue
		}

		lst.Entries[j] = en
		debug.Assert(j == 0 || en.Name > lst.Entries[j-1].Name)
		j++
	}
	lst.Entries = lst.Entries[:j]

	return nil
}

// write one chunk with the following framing header:
// - [u32 headerLen] bytepack[entryCount | first | last ]
// and payload:
// -[msgp-encoded LsoEntries]
func (r *XactNBI) writeChunk(num int, entries cmn.LsoEntries) error {
	chunk, err := r.ufest.NewChunk(num, r.lom)
	if err != nil {
		return err
	}
	chunkPath := chunk.Path()

	fh, err := r.lom.CreatePart(chunkPath)
	if err != nil {
		return err
	}

	ws := cos.NewWriteSizer(fh)
	defer func() {
		cos.Close(fh)
		if err != nil {
			_ = cos.RemoveFile(chunkPath)
		}
	}()

	// 1) header (bytepack) with a u32 length prefix
	hdr := makeInvChunkHdr(entries)

	ps := hdr.PackedSize()
	debug.Assert(ps < nbiMaxHdrLen, ps, " vs max ", nbiMaxHdrLen)
	debug.Assert(ps < len(r.buf), ps, " vs buf ", len(r.buf))

	hdrBuf := hdr.pack(r.buf)

	// write framing bytes
	var frame [nbiFrameSize]byte
	binary.BigEndian.PutUint32(frame[:], uint32(len(hdrBuf)))

	// checksum
	debug.Assert(r.cksum != nil && r.cksum.Ty() == cos.ChecksumCRC32C, "must have CRC32c")
	r.cksum.H.Reset()
	r.cksum.H.Write(hdrBuf)
	crc := r.cksum.SumTo()
	copy(frame[cos.SizeofI32:], crc)

	if _, err := ws.Write(frame[:]); err != nil {
		return err
	}

	// write header bytes
	if _, err := ws.Write(hdrBuf); err != nil {
		return err
	}

	// 2) payload: msgp-encoded entries (note: using the same buffer for hdr and msgp payload)
	mw := msgp.NewWriterBuf(ws, r.buf)
	if err := entries.EncodeMsg(mw); err != nil {
		return err
	}
	if err := mw.Flush(); err != nil {
		return err
	}

	written := ws.Size()
	if err := r.ufest.Add(chunk, written, int64(num)); err != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return err
	}

	return nil
}

// true if backend appended into dst's backing array (dst must have cap>0)
func _sameBacking(dst, got cmn.LsoEntries) bool {
	if len(got) == 0 {
		return true
	}
	if cap(dst) == 0 {
		return false
	}
	// dst is len=0, cap>0
	return &got[0] == &dst[:1][0]
}

/////////////////
// nbiChunkHdr //
/////////////////

func makeInvChunkHdr(entries cmn.LsoEntries) (h nbiChunkHdr) {
	h.entryCount = uint32(len(entries))
	if l := len(entries); l > 0 {
		h.first = entries[0].Name
		h.last = entries[l-1].Name
	}
	return h
}

func (h *nbiChunkHdr) pack(buf []byte) []byte {
	p := cos.NewPacker(buf, h.PackedSize())
	h.Pack(p)
	return p.Bytes()
}

func (h *nbiChunkHdr) PackedSize() int {
	// entryCount(uint32) + first + last
	return cos.SizeofI32 + cos.PackedStrLen(h.first) + cos.PackedStrLen(h.last)
}

func (h *nbiChunkHdr) Pack(p *cos.BytePack) {
	p.WriteUint32(h.entryCount)
	p.WriteString(h.first)
	p.WriteString(h.last)
}

func (h *nbiChunkHdr) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if h.entryCount, err = unpacker.ReadUint32(); err != nil {
		return err
	}
	if h.first, err = unpacker.ReadString(); err != nil {
		return err
	}
	h.last, err = unpacker.ReadString()
	return err
}
