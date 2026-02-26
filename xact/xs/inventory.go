// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	"github.com/tinylib/msgp/msgp"
)

// inventory chunk header meta-version
const (
	invChunkHdrVer uint8 = 1
)

// formatting - framing: per-chunk header
const (
	invHdrLenSize = cos.SizeofI32
)

type (
	invChunkHdr struct {
		ver        uint8
		entryCount uint32
		first      string
		last       string
	}
)

// x-inventory
type (
	invFactory struct {
		xreg.RenewBase
		xctn *XactInventory
	}
	XactInventory struct {
		xact.Base
		msg    *apc.CreateInvMsg
		lom    *core.LOM
		ufest  *core.Ufest
		buf    []byte
		slab   *memsys.Slab
		ctlmsg string
		clean  atomic.Bool // true: wi.cleanup() done _or_ recycled via mem-pool
	}
)

// TODO -- FIXME:
// - multi-target (remove smap.CountActiveTs = 1)

// interface guard
var (
	_ core.Xact      = (*XactInventory)(nil)
	_ xreg.Renewable = (*invFactory)(nil)
)

func (*invFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &invFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
}

func (p *invFactory) Start() error {
	bck := p.Bucket()

	debug.Assert(bck.IsRemote()) // guarded by proxy (case apc.ActCreateInventory)

	smap := core.T.Sowner().Get()
	if smap.CountActiveTs() > 1 {
		return cmn.NewErrNotImpl("create bucket inventory for", "multi-target cluster")
	}

	msg := p.Args.Custom.(*apc.CreateInvMsg)
	r := &XactInventory{msg: msg}
	r.InitBase(p.UUID(), p.Kind(), bck)

	// the name for a given bucket (intended for very large buckets)
	invName := r.msg.Name
	debug.Assert(invName != "")

	// inventory LOM is always chunked
	oname := string(r.Bck().MakeUname(invName))
	r.lom = core.AllocLOM(oname)

	if err := r.init(); err != nil {
		core.FreeLOM(r.lom)
		return err
	}

	_ = r.CtlMsg()
	p.xctn = r
	return nil
}

func (*invFactory) Kind() string     { return apc.ActCreateInventory }
func (p *invFactory) Get() core.Xact { return p.xctn }

func (p *invFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	if p.UUID() != prevEntry.UUID() {
		return wpr, cmn.NewErrXactUsePrev(prevEntry.Get().String())
	}
	bckEq := prevEntry.Bucket().Equal(p.Bucket(), true, true)
	debug.Assert(bckEq)
	return xreg.WprUse, nil
}

///////////////////
// XactInventory //
///////////////////

func (r *XactInventory) init() error {
	err := r.lom.InitBck(meta.SysBckInv())
	if err != nil {
		return err
	}
	uploadID := xact.PrefixInvID + cos.GenUUID()
	r.ufest, err = core.NewUfest(uploadID, r.lom, false /*must-exist*/)
	if err != nil {
		return err
	}
	r.buf, r.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)

	return nil
}

func (r *XactInventory) Abort(err error) bool {
	if !r.Base.Abort(err) {
		return false
	}
	r.cleanup()
	return true
}

func (r *XactInventory) cleanup() {
	if !r.clean.CAS(false, true) {
		return
	}
	defer core.FreeLOM(r.lom)
	r.slab.Free(r.buf)

	if r.IsAborted() {
		// cleanup partial
		r.ufest.Abort(r.lom)
	}
}

func (r *XactInventory) Run(wg *sync.WaitGroup) {
	wg.Done()

	nlog.Infoln(core.T.String(), "run:", r.Name(), "ctl:", r.ctlmsg)

	bck := r.Bck()
	lsmsg := &r.msg.LsoMsg
	if pgsize := bck.MaxPageSize(); lsmsg.PageSize <= 0 || lsmsg.PageSize > pgsize {
		lsmsg.PageSize = pgsize
	}
	// adjust to min
	if n := r.msg.MaxEntriesPerChunk; n > 0 {
		r.msg.PagesPerChunk = min(cos.DivRoundI64(n, lsmsg.PageSize), r.msg.PagesPerChunk)
	}
	lsmsg.ContinuationToken = ""

	var (
		bp        = core.T.Backend(bck)
		lastToken = "__dummy__"
		all       = make(cmn.LsoEntries, lsmsg.PageSize*r.msg.PagesPerChunk) // prealloc and reuse
	)
	for num := 1; !r.IsAborted() && lastToken != ""; num++ {
		var (
			idx    int
			npages int64
		)
		all = all[:cap(all)]
		for ; lastToken != "" && npages < r.msg.PagesPerChunk; npages++ {
			lst := &cmn.LsoRes{}
			lst.Entries = all[idx:]

			if _, err := bp.ListObjects(bck, lsmsg, lst); err != nil {
				r.Abort(err)
				return
			}

			// invariant: backend must reuse the passed-in slice ([:0] + append)
			debug.Assert(len(lst.Entries) == 0 || &lst.Entries[0] == &all[idx])

			idx += len(lst.Entries)
			lastToken = lst.ContinuationToken
			lsmsg.ContinuationToken = lastToken
		}
		if idx == 0 {
			break
		}

		// next chunk
		all = all[:idx]
		if err := r.writeChunk(num, all); err != nil {
			r.Abort(err)
			return
		}
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln(r.Name(), "listed entries:", idx, "npages:", npages, "token:", lastToken)
		}
	}
	if err := r.lom.CompleteUfest(r.ufest, false /*locked*/); err != nil {
		r.Abort(err)
		return
	}

	r.Finish()
	r.cleanup()
}

// writeChunk writes one chunk part file:
// [u32 headerLen][bytepack header][msgp-encoded LsoEntries]
func (r *XactInventory) writeChunk(num int, entries cmn.LsoEntries) error {
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
	hdrBytes := packInvChunkHdr(&hdr)

	// write headerLen prefix
	var lenbuf [invHdrLenSize]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(hdrBytes)))
	if _, err := ws.Write(lenbuf[:]); err != nil {
		return err
	}
	// write header bytes
	if _, err := ws.Write(hdrBytes); err != nil {
		return err
	}

	// 2) payload: msgp-encoded entries
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

func makeInvChunkHdr(entries cmn.LsoEntries) (h invChunkHdr) {
	h.ver = invChunkHdrVer
	h.entryCount = uint32(len(entries))
	if len(entries) > 0 {
		h.first = entries[0].Name
		h.last = entries[len(entries)-1].Name
	}
	return
}

func packInvChunkHdr(h *invChunkHdr) []byte {
	p := cos.NewPacker(nil, h.PackedSize())
	h.Pack(p)
	return p.Bytes()
}

func (h *invChunkHdr) PackedSize() int {
	// ver(uint8) + entryCount(uint32) + first + last
	return 1 + cos.SizeofI32 + cos.PackedStrLen(h.first) + cos.PackedStrLen(h.last)
}

func (h *invChunkHdr) Pack(p *cos.BytePack) {
	p.WriteUint8(h.ver)
	p.WriteUint32(h.entryCount)
	p.WriteString(h.first)
	p.WriteString(h.last)
}

func (r *XactInventory) CtlMsg() string {
	if r.ctlmsg != "" {
		return r.ctlmsg
	}
	sb := &cos.SB{}
	sb.Init(80)
	r.msg.Str(r.Bck().Cname(""), sb)
	r.ctlmsg = sb.String()
	return r.ctlmsg
}

func (r *XactInventory) Snap() (snap *core.Snap) {
	return r.Base.NewSnap(r)
}
