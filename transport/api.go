// Package transport provides long-lived http/tcp connections for intra-cluster communications
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

// ObjHdr represents the transport header passed to recv callbacks.
//
// NOTE: ObjHdr and all of its fields (especially []byte fields like Opaque and Demux)
// alias a temporary buffer (`it.hbuf`) reused on the next iteration of RxAnyStream.
// Therefore:
//
//   • DO NOT retain ObjHdr or its fields beyond the recv() call
//   • DO NOT pass ObjHdr to goroutines
//   • DO NOT store ObjHdr in long-lived structures
//
// Correct usage: consume ObjHdr synchronously and inline inside the recv() callback.

type (
	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	SentCB func(*ObjHdr, io.ReadCloser, any, error)

	// scope: stream
	// flow: connection dead => terminate => TermedCB => [reconnect() => fresh stream to same peer]
	TermedCB func(dstID string, err error)

	// usage and scope:
	// - entire stream's lifetime (all Send() calls)
	// - additional stream control
	// - global or optional params (to override defaults)
	Parent struct {
		Xact     core.Xact // sender ID; abort
		SentCB   SentCB    // to free SGLs, close files, etc. cleanup
		TermedCB TermedCB  // when err-ed
	}
	Extra struct {
		Parent       *Parent
		Config       *cmn.Config   // (to optimize-out GCO.Get())
		Compression  string        // see CompressAlways, etc. enum
		IdleTeardown time.Duration // when exceeded, causes PUT to terminate (and to renew upon the very next send)
		ChanBurst    int           // overrides config.Transport.Burst
		SizePDU      int32         // NOTE: 0(zero): no PDUs; must be <= `maxSizePDU`; unknown size _requires_ PDUs
		MaxHdrSize   int32         // overrides config.Transport.MaxHeaderSize
	}

	// _object_ header (not to confuse w/ objects in buckets)
	ObjHdr struct {
		Bck      cmn.Bck
		ObjName  string
		SID      string       // sender node ID
		Demux    string       // for shared data mover(s), to demux on the receive side
		Opaque   []byte       // custom control (optional)
		ObjAttrs cmn.ObjAttrs // attributes/metadata of the object that's being transmitted
		Opcode   int          // (see reserved range above)
	}
	// object to transmit
	Obj struct {
		Reader  io.ReadCloser // reader (to read the object, and close when done)
		CmplArg any           // optional context passed to the SentCB callback
		SentCB  SentCB        // called when the last byte is sent _or_ when the stream terminates (see term.reason)
		prc     *atomic.Int64 // private; if present, ref-counts so that we call SentCB only once
		Hdr     ObjHdr
	}

	// stream collector
	StreamCollector struct{}

	// Rx callbacks
	RecvObj func(hdr *ObjHdr, objReader io.Reader, err error) error
)

// shared data mover (SDM)
type (
	Receiver interface {
		ID() string
		RecvObj(hdr *ObjHdr, objReader io.Reader, err error) error // Rx callback above
	}
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
	if extra.Parent != nil {
		s.sentCB = extra.Parent.SentCB
	}
	if extra.Compressed() {
		s.initCompression(extra)
	}
	debug.Assert(s.usePDU() == extra.UsePDU())

	chsize := burst(extra)             // num objects the caller can post without blocking
	s.workCh = make(chan *Obj, chsize) // Send Queue (SQ)
	s.cmplCh = make(chan cmpl, chsize) // Send Completion Queue (SCQ)

	s.wg.Add(2)
	go s.sendLoop(extra.Config, dryrun()) // handle SQ
	go s.cmplLoop()                       // handle SCQ

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
//     or fails. On success, if send-completion (SentCB) callback is provided
//     (i.e., non-nil), the closing is done by doCmpl().
//   - Optional reference counting is also done by (and in) the doCmpl, so that the
//     SentCB gets called if and only when the refcount (if provided i.e., non-nil)
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

func Handle(trname string, rxObj RecvObj) error {
	h := &handler{trname: trname, rxObj: rxObj}
	return oput(trname, h)
}

func Unhandle(trname string) error { return odel(trname) }

////////////////////
// stats and misc //
////////////////////

func ObjURLPath(trname string) string { return _urlPath(apc.ObjStream, trname) }

func _urlPath(endp, trname string) string {
	if trname == "" {
		return cos.JoinW0(apc.Version, endp)
	}
	return cos.JoinW0(apc.Version, endp, trname)
}
