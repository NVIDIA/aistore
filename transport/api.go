// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"math"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
)

///////////////////
// object stream //
///////////////////

// range of 16 `Obj.Hdr.Opcode` and `Msg.Opcode` values
// reserved for _internal_ use
const (
	opcFin = iota + math.MaxUint16 - 16
	opcIdleTick
)

func ReservedOpcode(opc int) bool { return opc >= opcFin }

const (
	SizeUnknown = cos.ContentLengthUnknown // -1: obj size unknown (not set)

	dfltSizePDU = memsys.DefaultBufSize
	maxSizePDU  = memsys.MaxPageSlabSize

	// see also: cmn/config for (max, default) transport header sizes
)

const sizeofh = int(unsafe.Sizeof(Obj{}))

type (
	// advanced usage: additional stream control
	Extra struct {
		Xact         core.Xact     // usage: sender ID; abort
		Callback     ObjSentCB     // typical usage: to free SGLs, close files
		Config       *cmn.Config   // (to optimize-out GCO.Get())
		Compression  string        // see CompressAlways, etc. enum
		IdleTeardown time.Duration // when exceeded, causes PUT to terminate (and to renew upon the very next send)
		ChanBurst    int           // overrides config.Transport.Burst
		SizePDU      int32         // NOTE: 0(zero): no PDUs; must be <= `maxSizePDU`; unknown size _requires_ PDUs
		MaxHdrSize   int32         // overrides config.Transport.MaxHeaderSize
	}

	// receive-side session stats indexed by session ID (see recv.go for "uid")
	// optional, currently tests only
	RxStats map[uint64]*Stats

	// object header
	ObjHdr struct {
		Bck      cmn.Bck
		ObjName  string
		SID      string       // sender node ID
		Opaque   []byte       // custom control (optional)
		ObjAttrs cmn.ObjAttrs // attributes/metadata of the object that's being transmitted
		Opcode   int          // (see reserved range above)
	}
	// object to transmit
	Obj struct {
		Reader   io.ReadCloser // reader (to read the object, and close when done)
		CmplArg  any           // optional context passed to the ObjSentCB callback
		Callback ObjSentCB     // called when the last byte is sent _or_ when the stream terminates (see term.reason)
		prc      *atomic.Int64 // private; if present, ref-counts so that we call ObjSentCB only once
		Hdr      ObjHdr
	}

	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	ObjSentCB func(*ObjHdr, io.ReadCloser, any, error)

	Msg struct {
		SID    string
		Body   []byte
		Opcode int
	}

	// stream collector
	StreamCollector struct{}

	// Rx callbacks
	RecvObj func(hdr *ObjHdr, objReader io.Reader, err error) error
	RecvMsg func(msg Msg, err error) error
)

///////////////////
// object stream //
///////////////////

func NewObjStream(client Client, dstURL, dstID string, extra *Extra) (s *Stream) {
	if extra == nil {
		extra = &Extra{Config: cmn.GCO.Get()}
	} else if extra.Config == nil {
		extra.Config = cmn.GCO.Get()
	}
	s = &Stream{streamBase: *newBase(client, dstURL, dstID, extra)}
	s.streamBase.streamer = s
	s.callback = extra.Callback
	if extra.Compressed() {
		s.initCompression(extra)
	}
	debug.Assert(s.usePDU() == extra.UsePDU())

	chsize := burst(extra)             // num objects the caller can post without blocking
	s.workCh = make(chan *Obj, chsize) // Send Qeueue (SQ)
	s.cmplCh = make(chan cmpl, chsize) // Send Completion Queue (SCQ)

	s.wg.Add(2)
	go s.sendLoop(dryrun()) // handle SQ
	go s.cmplLoop()         // handle SCQ

	gc.ctrlCh <- ctrl{&s.streamBase, true /* collect */}
	return
}

// Asynchronously send an object (transport.Obj) defined by its header and its reader.
//
// The sending pipeline is implemented as a pair (SQ, SCQ) where the former is a send
// queue realized as workCh, and the latter is a send completion queue (cmplCh).
// Together SQ and SCQ form a FIFO.
//
//   - header-only objects are supported; when there's no data to send (that is,
//     when the header's Dsize field is set to zero), the reader is not required and the
//     corresponding argument in Send() can be set to nil.
//   - object reader is *always* closed irrespectively of whether the Send() succeeds
//     or fails. On success, if send-completion (ObjSentCB) callback is provided
//     (i.e., non-nil), the closing is done by doCmpl().
//   - Optional reference counting is also done by (and in) the doCmpl, so that the
//     ObjSentCB gets called if and only when the refcount (if provided i.e., non-nil)
//     reaches zero.
//   - For every transmission of every object there's always an doCmpl() completion
//     (with its refcounting and reader-closing). This holds true in all cases including
//     network errors that may cause sudden and instant termination of the underlying
//     stream(s).
func (s *Stream) Send(obj *Obj) (err error) {
	debug.Assertf(len(obj.Hdr.Opaque) < len(s.maxhdr)-sizeofh, "(%d, %d)", len(obj.Hdr.Opaque), len(s.maxhdr))
	if err = s.startSend(obj); err != nil {
		s.doCmpl(obj, err) // take a shortcut
		return
	}

	l, c := len(s.workCh), cap(s.workCh)
	s.chanFull.Check(l, c)

	s.workCh <- obj

	return
}

func (s *Stream) Fin() {
	_ = s.Send(&Obj{Hdr: ObjHdr{Opcode: opcFin}})
	s.wg.Wait()
}

//////////////////////
// receive-side API //
//////////////////////

func Handle(trname string, rxObj RecvObj, withStats ...bool) error {
	var h handler
	if len(withStats) > 0 && withStats[0] {
		hkName := ObjURLPath(trname)
		hex := &hdlExtra{hdl: hdl{trname: trname, rxObj: rxObj}, hkName: hkName}
		hk.Reg(hkName+hk.NameSuffix, hex.cleanup, sessionIsOld)
		h = hex
	} else {
		h = &hdl{trname: trname, rxObj: rxObj}
	}
	return oput(trname, h)
}

func Unhandle(trname string) error { return odel(trname) }

////////////////////
// stats and misc //
////////////////////

func ObjURLPath(trname string) string { return _urlPath(apc.ObjStream, trname) }

func _urlPath(endp, trname string) string {
	if trname == "" {
		return cos.JoinWords(apc.Version, endp)
	}
	return cos.JoinWords(apc.Version, endp, trname)
}

func GetRxStats() (netstats map[string]RxStats) {
	netstats = make(map[string]RxStats)
	for i, hmap := range hmaps {
		hmtxs[i].Lock()
		for trname, h := range hmap {
			if s := h.getStats(); s != nil {
				netstats[trname] = s
			}
		}
		hmtxs[i].Unlock()
	}
	return
}
