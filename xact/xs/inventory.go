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

const (
	PrefixInvID = "inv-"

	// inventory chunk header meta-version
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
	}
)

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

	// TODO -- FIXME: remove the following two checks
	if !bck.IsCloud() {
		return cmn.NewErrNotImpl("create inventory for", bck.Provider+" bucket")
	}
	smap := core.T.Sowner().Get()
	if smap.CountActiveTs() > 1 {
		return cmn.NewErrNotImpl("create bucket inventory for", "multi-target cluster")
	}

	msg := p.Args.Custom.(*apc.CreateInvMsg)
	if err := msg.Validate(); err != nil { // TODO -- FIXME: unify validation
		return err
	}
	r := &XactInventory{msg: msg}
	r.InitBase(p.UUID(), p.Kind(), bck)

	// the name for a given bucket
	invName := r.msg.Name
	if invName == "" {
		invName = PrefixInvID + r.ID()
	}

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
	uploadID := PrefixInvID + cos.GenUUID()
	r.ufest, err = core.NewUfest(uploadID, r.lom, false /*must-exist*/)
	if err != nil {
		return err
	}
	r.buf, r.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)

	return nil
}

// TODO -- FIXME: cleanup vs. 1) TxnAbort, b) user abort
func (r *XactInventory) Abort(err error) bool {
	if !r.Base.Abort(err) {
		return false
	}
	r.cleanup()
	return true
}

func (r *XactInventory) cleanup() {
	defer core.FreeLOM(r.lom)
	r.slab.Free(r.buf)

	if !r.IsAborted() {
		return
	}
	r.ufest.Abort(r.lom)
}

// TODO: single return at the bottom vs r.cleanup() :TODO
func (r *XactInventory) Run(wg *sync.WaitGroup) {
	wg.Done()

	nlog.Infoln(core.T.String(), "run:", r.Name(), "ctl:", r.ctlmsg)

	bck := r.Bck()
	lsmsg := r.msg.LsoMsg
	if pgsize := bck.MaxPageSize(); lsmsg.PageSize <= 0 || lsmsg.PageSize > pgsize {
		lsmsg.PageSize = pgsize
	}
	lsmsg.ContinuationToken = ""

	bp := core.T.Backend(bck)
outer:
	for num := 1; !r.IsAborted(); num++ {
		// accumulate up to pagesPerChunk pages into one slice
		var (
			all       cmn.LsoEntries
			npages    int64
			lastToken string
		)
		all = make(cmn.LsoEntries, 0, lsmsg.PageSize*r.msg.PagesPerChunk)

		for npages < r.msg.PagesPerChunk {
			lst := &cmn.LsoRes{} // TODO -- FIXME: reuse entries slice pool
			_, err := bp.ListObjects(bck, &lsmsg, lst)
			if err != nil {
				r.Abort(err)
				break outer
			}

			all = append(all, lst.Entries...)
			npages++

			lastToken = lst.ContinuationToken
			if lastToken == "" {
				break // end of listing
			}
			lsmsg.ContinuationToken = lastToken
		}

		if len(all) == 0 {
			break
		}

		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infoln("inventory", r.ID(), "listed entries:", len(all), "npages:", npages, "token:", lastToken)
		}

		if _, err := r.writeChunk(num, all); err != nil {
			r.Abort(err)
			break
		}

		if lastToken == "" {
			break
		}
	}

	if !r.IsAborted() {
		if err := r.lom.CompleteUfest(r.ufest, false /*locked*/); err != nil {
			r.Abort(err)
		} else {
			r.Finish()
			r.cleanup()
		}
	}
}

// writeChunk writes one chunk part file:
// [u32 headerLen][bytepack header][msgp-encoded LsoEntries]
func (r *XactInventory) writeChunk(num int, entries cmn.LsoEntries) (written int64, err error) {
	chunk, err := r.ufest.NewChunk(num, r.lom)
	if err != nil {
		return 0, err
	}
	chunkPath := chunk.Path()

	fh, err := r.lom.CreatePart(chunkPath)
	if err != nil {
		return 0, err
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
	if _, err = ws.Write(lenbuf[:]); err != nil {
		return 0, err
	}
	// write header bytes
	if _, err = ws.Write(hdrBytes); err != nil {
		return 0, err
	}

	// 2) payload: msgp-encoded entries
	mw := msgp.NewWriterBuf(ws, r.buf)
	if err = entries.EncodeMsg(mw); err == nil {
		err = mw.Flush()
	}
	if err != nil {
		return 0, err
	}

	written = ws.Size()
	if err := r.ufest.Add(chunk, written, int64(num)); err != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return 0, err
	}

	return written, nil
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
