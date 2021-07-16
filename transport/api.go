// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
)

///////////////////
// object stream //
///////////////////

const (
	SizeUnknown    = -1
	DefaultSizePDU = memsys.DefaultBufSize
	MaxSizePDU     = memsys.MaxPageSlabSize
)

type (
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Callback    ObjSentCB     // typical usage: to free SGLs, close files, etc.
		Compression string        // see CompressAlways, etc. enum
		MMSA        *memsys.MMSA  // compression-related buffering
		Config      *cmn.Config   // config
		SizePDU     int32         // 0(zero): no PDUs; must be below MaxSizePDU; unknown size _requires_ PDUs
	}
	// stream stats
	Stats struct {
		Num            atomic.Int64 // number of transferred objects including zero size (header-only) objects
		Size           atomic.Int64 // transferred object size (does not include transport headers)
		Offset         atomic.Int64 // stream offset, in bytes
		CompressedSize atomic.Int64 // compressed size (NOTE: converges to the actual compressed size over time)
	}
	EndpointStats map[uint64]*Stats // all stats for a given (network, trname) endpoint indexed by session ID

	// object header
	ObjHdr struct {
		Bck      cmn.Bck
		ObjName  string
		ObjAttrs cmn.ObjAttrs // attributes/metadata of the sent object
		Opaque   []byte       // custom control (optional)
		SID      string       // sender node ID
		Opcode   int          // custom opcode (optional)
	}
	// object to transmit
	Obj struct {
		Hdr      ObjHdr        // object header
		Reader   io.ReadCloser // reader, to read the object, and close when done
		Callback ObjSentCB     // fired when sending is done OR when the stream terminates (see term.reason)
		CmplArg  interface{}   // Additional parameter which will be passed to the callback.
		prc      *atomic.Int64 // private; if present, ref-counts to call ObjSentCB only once
	}

	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	ObjSentCB func(ObjHdr, io.ReadCloser, interface{}, error)

	Msg struct {
		Flags int64
		Body  []byte
	}

	// stream collector
	StreamCollector struct{}

	// Rx callbacks
	ReceiveObj func(hdr ObjHdr, object io.Reader, err error)
	ReceiveMsg func(msg Msg, err error)
)

// see also: lom.FullName()
func (hdr *ObjHdr) FullName() string { return filepath.Join(hdr.Bck.Name, hdr.ObjName) }

///////////////////
// object stream //
///////////////////

func NewObjStream(client Client, toURL string, extra *Extra) (s *Stream) {
	s = &Stream{streamBase: *newStreamBase(client, toURL, extra)}
	s.streamBase.streamer = s
	if extra != nil {
		s.callback = extra.Callback
		if extra.Compressed() {
			s.initCompression(extra)
		}
	}
	burst := burst()                  // num objects the caller can post without blocking
	s.workCh = make(chan *Obj, burst) // Send Qeueue (SQ)
	s.cmplCh = make(chan cmpl, burst) // Send Completion Queue (SCQ)

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
// * header-only objects are supported; when there's no data to send (that is,
//   when the header's Dsize field is set to zero), the reader is not required and the
//   corresponding argument in Send() can be set to nil.
// * object reader is *always* closed irrespectively of whether the Send() succeeds
//   or fails. On success, if send-completion (ObjSentCB) callback is provided
//   (i.e., non-nil), the closing is done by doCmpl().
// * Optional reference counting is also done by (and in) the doCmpl, so that the
//   ObjSentCB gets called if and only when the refcount (if provided i.e., non-nil)
//   reaches zero.
// * For every transmission of every object there's always an doCmpl() completion
//   (with its refcounting and reader-closing). This holds true in all cases including
//   network errors that may cause sudden and instant termination of the underlying
//   stream(s).
func (s *Stream) Send(obj *Obj) (err error) {
	debug.Assert(len(obj.Hdr.Opaque) < len(s.maxheader)-int(unsafe.Sizeof(Obj{})))

	if err = s.startSend(obj); err != nil {
		s.doCmpl(obj, err) // take a shortcut
		return
	}

	// reader == nil iff is-header-only
	debug.Func(func() {
		if obj.Reader == nil {
			debug.Assert(obj.IsHeaderOnly())
		} else if obj.IsHeaderOnly() {
			val := reflect.ValueOf(obj.Reader)
			debug.AssertMsg(val.IsNil(), obj.String())
		}
	})

	s.workCh <- obj
	if verbose {
		glog.Infof("%s: send %s[sq=%d]", s, obj, len(s.workCh))
	}
	return
}

func (s *Stream) Fin() {
	_ = s.Send(&Obj{Hdr: ObjHdr{ObjAttrs: cmn.ObjAttrs{Size: lastMarker}}})
	s.wg.Wait()
}

////////////////////
// message stream //
////////////////////

func NewMsgStream(client Client, toURL string) (s *MsgStream) {
	s = &MsgStream{streamBase: *newStreamBase(client, toURL, nil)}
	s.streamBase.streamer = s

	burst := burst()                  // num messages the caller can post without blocking
	s.workCh = make(chan *Msg, burst) // Send Qeueue or SQ

	s.wg.Add(1)
	go s.sendLoop(dryrun())

	gc.ctrlCh <- ctrl{&s.streamBase, true /* collect */}
	return
}

func (s *MsgStream) Send(msg *Msg) (err error) {
	debug.Assert(len(msg.Body) < len(s.maxheader)-int(unsafe.Sizeof(Msg{})))
	if err = s.startSend(msg); err != nil {
		return
	}
	s.workCh <- msg
	if verbose {
		glog.Infof("%s: send %s[sq=%d]", s, msg, len(s.workCh))
	}
	return
}

func (s *MsgStream) Fin() {
	_ = s.Send(&Msg{Flags: lastMarker})
	s.wg.Wait()
}

//////////////////////
// receive-side API //
//////////////////////

func HandleObjStream(trname string, rxObj ReceiveObj, mems ...*memsys.MMSA) error {
	var mm *memsys.MMSA
	if len(mems) > 0 {
		mm = mems[0]
	} else {
		mm = memsys.DefaultPageMM()
	}
	h := &handler{trname: trname, rxObj: rxObj, hkName: ObjURLPath(trname), mm: mm}
	return h.handle()
}

func HandleMsgStream(trname string, rxMsg ReceiveMsg) error {
	mm := memsys.DefaultSmallMM()
	h := &handler{trname: trname, rxMsg: rxMsg, hkName: MsgURLPath(trname), mm: mm}
	return h.handle()
}

func Unhandle(trname string) (err error) {
	mu.Lock()
	if h, ok := handlers[trname]; ok {
		delete(handlers, trname)
		mu.Unlock()
		hk.Unreg(h.hkName)
	} else {
		mu.Unlock()
		err = fmt.Errorf(cmn.FmtErrUnknown, "transport", "endpoint", trname)
	}
	return
}

////////////////////
// stats and misc //
////////////////////

func ObjURLPath(trname string) string { return _urlPath(cmn.ObjStream, trname) }
func MsgURLPath(trname string) string { return _urlPath(cmn.MsgStream, trname) }

func _urlPath(endp, trname string) string {
	if trname == "" {
		return cos.JoinWords(cmn.Version, endp)
	}
	return cos.JoinWords(cmn.Version, endp, trname)
}

func GetStats() (netstats map[string]EndpointStats, err error) {
	netstats = make(map[string]EndpointStats)
	mu.Lock()
	for trname, h := range handlers {
		eps := make(EndpointStats)
		f := func(key, value interface{}) bool {
			out := &Stats{}
			uid := key.(uint64)
			in := value.(*Stats)
			out.Num.Store(in.Num.Load())
			out.Offset.Store(in.Offset.Load())
			out.Size.Store(in.Size.Load())
			eps[uid] = out
			return true
		}
		h.sessions.Range(f)
		netstats[trname] = eps
	}
	mu.Unlock()
	return
}
