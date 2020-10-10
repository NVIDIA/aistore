// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"io/ioutil"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	Stream struct {
		workCh   chan streamable // aka SQ: next object to stream
		cmplCh   chan cmpl       // aka SCQ; note that SQ and SCQ together form a FIFO
		callback ObjSentCB       // to free SGLs, close files, etc.
		sendoff  sendoff
		lz4s     lz4Stream
		streamBase
	}
	// advanced usage: additional stream control
	Extra struct {
		IdleTimeout time.Duration // stream idle timeout: causes PUT to terminate (and renew on the next obj send)
		Callback    ObjSentCB     // typical usage: to free SGLs, close files, etc.
		Compression string        // see CompressAlways, etc. enum
		MMSA        *memsys.MMSA  // compression-related buffering
		Config      *cmn.Config
	}
	// stream stats
	Stats struct {
		Num            atomic.Int64 // number of transferred objects including zero size (header-only) objects
		Size           atomic.Int64 // transferred object size (does not include transport headers)
		Offset         atomic.Int64 // stream offset, in bytes
		CompressedSize atomic.Int64 // compressed size (NOTE: converges to the actual compressed size over time)
	}
	EndpointStats map[uint64]*Stats // all stats for a given http endpoint defined by a tuple(network, trname) by session ID

	// object attrs
	ObjectAttrs struct {
		Atime      int64  // access time - nanoseconds since UNIX epoch
		Size       int64  // size of objects in bytes
		CksumType  string // checksum type
		CksumValue string // checksum of the object produced by given checksum type
		Version    string // version of the object
	}
	// object header
	ObjHdr struct {
		Bck      cmn.Bck
		ObjName  string
		ObjAttrs ObjectAttrs // attributes/metadata of the sent object
		Opaque   []byte      // custom control (optional)
	}
	// object to transmit
	Obj struct {
		Hdr      ObjHdr         // object header
		Reader   io.ReadCloser  // reader, to read the object, and close when done
		Callback ObjSentCB      // callback fired when sending is done OR when the stream terminates (see term.reason)
		CmplPtr  unsafe.Pointer // local pointer that gets returned to the caller via Send completion callback
		// private
		prc *atomic.Int64 // if present, ref-counts num sent objects to call SendCallback only once
	}
	MsgHdr struct {
		RecvHandler string
		KVS         cmn.SimpleKVs
	}
	Msg struct {
		Hdr  MsgHdr
		Body []byte
	}

	// object-sent callback that has the following signature can optionally be defined on a:
	// a) per-stream basis (via NewStream constructor - see Extra struct above)
	// b) for a given object that is being sent (for instance, to support a call-per-batch semantics)
	// Naturally, object callback "overrides" the per-stream one: when object callback is defined
	// (i.e., non-nil), the stream callback is ignored/skipped.
	// NOTE: if defined, the callback executes asynchronously as far as the sending part is concerned
	ObjSentCB func(ObjHdr, io.ReadCloser, unsafe.Pointer, error)

	StreamCollector struct {
		cmn.Named
	}
)

func NewStream(client Client, toURL string, extra *Extra) (s *Stream) {
	s = &Stream{streamBase: *newStreamBase(client, toURL, extra)}

	if extra != nil {
		s.callback = extra.Callback
		if extra.Compressed() {
			s.initCompression(extra)
		}
	}

	// burst size: the number of objects the caller is permitted to post for sending
	// without experiencing any sort of back-pressure
	burst := burst()
	s.workCh = make(chan streamable, burst) // Send Qeueue or SQ
	s.cmplCh = make(chan cmpl, burst)       // Send Completion Queue or SCQ

	s.wg.Add(2)
	go s.sendLoop(dryrun()) // handle SQ
	go s.cmplLoop()         // handle SCQ

	gc.ctrlCh <- ctrl{s, true /* collect */}
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
// * object reader is always closed by the code that handles send completions.
//   In the case when SendCallback is provided (i.e., non-nil), the closing is done
//   right after calling this callback - see objDone below for details.
// * Optional reference counting is also done by (and in) the objDone, so that the
//   SendCallback gets called if and only when the refcount (if provided i.e., non-nil)
//   reaches zero.
// * For every transmission of every object there's always an objDone() completion
//   (with its refcounting and reader-closing). This holds true in all cases including
//   network errors that may cause sudden and instant termination of the underlying
//   stream(s).
func (s *Stream) Send(obj Obj) (err error) {
	var (
		hdr     = &obj.Hdr
		verbose = bool(glog.FastV(4, glog.SmoduleTransport))
	)
	if err = s.startSend(hdr, verbose); err != nil {
		return
	}
	if obj.Reader == nil {
		debug.Assert(hdr.IsHeaderOnly())
		obj.Reader = nopRC
	} else if debug.Enabled && hdr.IsHeaderOnly() {
		b, _ := ioutil.ReadAll(obj.Reader)
		debug.Assert(len(b) == 0)
	}
	s.workCh <- obj
	if verbose {
		glog.Infof("%s: send %s[sq=%d]", s, hdr, len(s.workCh))
	}
	return
}

func (s *Stream) SendMsg(msg Msg) (err error) {
	var (
		hdr     = &msg.Hdr
		verbose = bool(glog.FastV(4, glog.SmoduleTransport))
	)
	if err = s.startSend(hdr, verbose); err != nil {
		return
	}
	s.workCh <- msg
	if verbose {
		glog.Infof("%s: sendmsg %s[sq=%d]", s, hdr, len(s.workCh))
	}
	return
}

func (s *Stream) Fin() {
	_ = s.Send(Obj{Hdr: ObjHdr{ObjAttrs: ObjectAttrs{Size: lastMarker}}})
	s.wg.Wait()
}
