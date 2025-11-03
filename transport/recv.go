// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"runtime"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/pierrec/lz4/v4"
)

// TODO:
// - see "amend all callers" below
// - err.IsBenign()

// private types
type (
	handler struct {
		rxObj  RecvObj
		trname string
	}
	iterator struct {
		body    io.Reader
		handler *handler
		pdu     *rpdu
		hbuf    []byte
		sid     string
		loghdr  string
	}
	objReader struct {
		body   io.Reader
		pdu    *rpdu
		parent *iterator
		hdr    ObjHdr
		off    int64
	}
)

// global
var (
	nextSessionID atomic.Int64 // next unique session ID
)

// main Rx objects
func RxAnyStream(w http.ResponseWriter, r *http.Request) {
	var (
		reader    io.Reader = r.Body
		lz4Reader *lz4.Reader
		trname    = path.Base(r.URL.Path)
		mm        = memsys.PageMM()
	)
	// Rx handler
	h, err := oget(trname)
	if err != nil {
		//
		//  Try reading `nextProtoHdr` containing transport.ObjHdr -
		//  that's because low-level `Stream.Fin` (sending graceful `opcFin`)
		//  could be the cause for `errUnknownTrname` and `errAlreadyClosedTrname`.
		//  Secondly, `errAlreadyClosedTrname` is considered benign, attributed
		//  to xaction abort and such - the fact that'd be difficult to confirm
		//  at the lowest level (and with no handler and its rxObj cb).
		//
		if _, ok := err.(*errAlreadyClosedTrname); ok {
			if cmn.Rom.V(5, cos.ModTransport) {
				nlog.Errorln(trname, "err:", err)
			}
		} else {
			cmn.WriteErr(w, r, err, 0)
		}
		return
	}
	// compression
	if compressionType := r.Header.Get(apc.HdrCompress); compressionType != "" {
		debug.Assert(compressionType == apc.LZ4Compression)
		lz4Reader = lz4.NewReader(r.Body)
		reader = lz4Reader
	}

	var (
		config = cmn.GCO.Get()
		it     = &iterator{
			handler: h,
			body:    reader,
			sid:     r.Header.Get(apc.HdrSenderID),
		}
	)
	debug.Assert(config.Transport.IdleTeardown > 0, "invalid config ", config.Transport)
	it.hbuf, _ = mm.AllocSize(_sizeHdr(config, 0))

	// receive loop (until eof or error)
	it.loghdr = _loghdr(h.trname, it.sid, core.T.SID(), false /*transmit*/, lz4Reader != nil)
	err = it.rxloop(mm)

	// cleanup
	if lz4Reader != nil {
		lz4Reader.Reset(nil)
	}
	if it.pdu != nil {
		it.pdu.free(mm)
	}
	mm.Free(it.hbuf)

	if !cos.IsOkEOF(err) {
		cmn.WriteErr(w, r, err)
	}
}

////////////////
// Rx handler //
////////////////

// begin t2t session
func (h *handler) recv(hdr *ObjHdr, objReader io.Reader, err error) error {
	return h.rxObj(hdr, objReader, err)
}

//////////////////////////////////
// next(obj, msg, pdu) iterator //
//////////////////////////////////

func (it *iterator) Read(p []byte) (n int, err error) { return it.body.Read(p) }

func (it *iterator) rxloop(mm *memsys.MMSA) (err error) {
	for err == nil {
		var (
			flags uint64
			hlen  int
		)
		hlen, flags, err = it.nextProtoHdr()

		// Protocol header read - first line of defense for transport errors.
		// Note that io.EOF from sender's Stream.Fin() (opcFin) is normal termination
		// (hence, cos.IsOkEOF() filter below).
		//
		// We do call recv() for all other errors (connection reset, malformed headers,
		// unexpected EOF, etc.) so receivers can see and handle transport failures.
		//
		// Compare w/ recv() callbacks below that handle object-header and object-payload
		// level errors.

		if err != nil {
			if !cos.IsOkEOF(err) { //
				if errCb := it.handler.recv(&ObjHdr{SID: it.sid}, nil, err); errCb != nil {
					err = errCb
				}
			}
			break
		}
		if hlen > cap(it.hbuf) {
			if hlen > cmn.MaxTransportHeader {
				err = it.newErr(err, sbrProtoHdrTooLong, fmt.Sprintf("%d>%d", hlen, cmn.MaxTransportHeader))
				break
			}
			// grow
			nlog.Warningf("%s: header length %d exceeds the current buffer %d", it.loghdr, hlen, cap(it.hbuf))
			mm.Free(it.hbuf)
			it.hbuf, _ = mm.AllocSize(min(int64(hlen)<<1, cmn.MaxTransportHeader))
		}

		debug.Assert(flags&msgFl == 0) //  messaging: not used, removed
		if flags&pduStreamFl != 0 {
			if it.pdu == nil {
				pbuf, _ := mm.AllocSize(maxSizePDU)
				it.pdu = newRecvPDU(it, pbuf)
			} else {
				it.pdu.reset()
			}
		}
		err = it.rxObj(hlen)
	}

	return err
}

func (it *iterator) rxObj(hlen int) (err error) {
	var (
		obj *objReader
		h   = it.handler
	)
	obj, err = it.nextObj(hlen)
	if obj != nil {
		if !obj.hdr.IsHeaderOnly() {
			obj.pdu = it.pdu
		}
		err = eofOK(err)
		size, off := obj.hdr.ObjAttrs.Size, obj.off
		if errCb := h.recv(&obj.hdr, obj, err); errCb != nil {
			err = errCb
		}
		debug.DeadBeefSmall(it.hbuf[:hlen])
		// stats
		if err == nil {
			g.tstats.Inc(cos.StreamsInObjCount) // stats/target_stats.go

			if size >= 0 {
				g.tstats.Add(cos.StreamsInObjSize, size)
			} else {
				debug.Assert(size == SizeUnknown)
				g.tstats.Add(cos.StreamsInObjSize, obj.off-off)
			}
		}
	} else if err != nil && err != io.EOF {
		if errCb := h.recv(&ObjHdr{SID: it.sid}, nil, err); errCb != nil {
			err = errCb
		}
	}
	return err
}

func eofOK(err error) error {
	if err == io.EOF {
		err = nil
	}
	return err
}

// nextProtoHdr receives and handles 16 bytes of the protocol header (not to confuse with transport.Obj.Hdr)
// returns:
// - hlen: header length for transport.Obj (and formerly, message length for transport.Msg)
// - flags: msgFl | pduFl | pduLastFl | pduStreamFl
// - error
func (it *iterator) nextProtoHdr() (int, uint64, error) {
	n, err := it.Read(it.hbuf[:sizeProtoHdr])
	if n < sizeProtoHdr {
		switch {
		case err == nil:
			err = it.newErr(io.ErrUnexpectedEOF, sbrProtoHdr, fmt.Sprintf("n=%d", n))
		case n == 0 && cos.IsOkEOF(err):
			// ok
		case n == 0:
			err = it.newErr(err, sbrProtoHdr, "")
		default:
			err = it.newErr(err, sbrProtoHdr, fmt.Sprintf("n=%d", n))
		}
		return 0, 0, err
	}
	// extract and validate hlen
	return it.extProtoHdr(it.hbuf)
}

func (it *iterator) nextObj(hlen int) (*objReader, error) {
	n, err := it.Read(it.hbuf[:hlen])
	if n < hlen {
		if err == nil {
			// [retry] insist on receiving the full length
			var m int
			for range maxInReadRetries {
				runtime.Gosched()
				m, err = it.Read(it.hbuf[n:hlen])
				if err != nil {
					break
				}
				// Check for potential overflow before adding
				debug.Assert(n <= math.MaxInt-m)
				n += m
				if n >= hlen {
					debug.Assert(n == hlen)
					break
				}
			}
		}
		if n < hlen {
			if err == nil || errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, it.newErr(err, sbrObjHdrTooShort, fmt.Sprintf("%d<%d", n, hlen))
		}
	}
	hdr := ExtObjHeader(it.hbuf, hlen)
	if hdr.isFin() {
		return nil, io.EOF
	}

	obj := allocRecv()
	obj.body, obj.hdr, obj.parent = it.body, hdr, it
	return obj, nil
}

func (it *iterator) newErr(err error, code, ctx string) error {
	return &ErrSBR{
		err:    err,
		loghdr: it.loghdr,
		sid:    it.sid,
		code:   code,
		ctx:    ctx,
	}
}

///////////////
// objReader //
///////////////

func (obj *objReader) Read(b []byte) (n int, err error) {
	if obj.pdu != nil {
		return obj.readPDU(b)
	}
	debug.Assert(obj.Size() >= 0)

	rem := obj.Size() - obj.off
	if rem < int64(len(b)) && rem >= 0 {
		b = b[:int(rem)]
	}

	n, err = obj.body.Read(b)
	obj.off += int64(n) // NOTE: `GORACE` complaining here can be safely ignored

	switch err {
	case nil:
		if obj.off >= obj.Size() {
			// ok (w/ EOF to the caller)
			err = io.EOF
		}
	case io.EOF:
		// premature EOF
		if obj.off != obj.Size() {
			err = obj.parent.newErr(io.ErrUnexpectedEOF, sbrObjDataEOF, obj.String())
		}
	default:
		// ditto, w/ error
		if obj.off < obj.Size() {
			err = obj.parent.newErr(err, sbrObjDataSize, obj.String())
		} else {
			err = obj.parent.newErr(err, sbrObjData, obj.String())
		}
	}
	return n, err
}

func (obj *objReader) String() string {
	return fmt.Sprintf("%s(size=%d)", obj.hdr.Cname(), obj.Size())
}

func (obj *objReader) Size() int64     { return obj.hdr.ObjSize() }
func (obj *objReader) IsUnsized() bool { return obj.hdr.IsUnsized() }

//
// pduReader
//

func (obj *objReader) readPDU(b []byte) (n int, err error) {
	pdu := obj.pdu
	if pdu.woff == 0 {
		err = pdu.readHdr()
		if err != nil {
			return 0, err
		}
	}
	for !pdu.done {
		if _, err = pdu.readFrom(); err != nil && err != io.EOF {
			err = obj.parent.newErr(err, sbrPDUData, obj.String())
			break
		}
		debug.Assert(err == nil || (err == io.EOF && pdu.done))
		if !pdu.done {
			runtime.Gosched()
		}
	}
	n = pdu.read(b)
	obj.off += int64(n)

	if err != nil {
		return n, err
	}
	if pdu.rlength() == 0 {
		if pdu.last {
			err = io.EOF
			if obj.IsUnsized() {
				obj.hdr.ObjAttrs.Size = obj.off
			} else if obj.Size() != obj.off {
				err = obj.parent.newErr(io.ErrUnexpectedEOF, sbrPDUDataSize, obj.String())
			}
		} else {
			pdu.reset()
		}
	}
	return n, err
}

// DrainAndFreeReader:
// 1) reads and discards all the data from `r` - the `objReader`;
// 2) frees this objReader back to the `recvPool`.
// As such, this function is intended for usage only and exclusively by
// `transport.RecvObj` implementations.
func DrainAndFreeReader(r io.Reader) {
	if r == nil {
		return
	}
	obj, ok := r.(*objReader)
	debug.Assert(ok)
	if obj.body != nil && !obj.hdr.IsHeaderOnly() {
		cos.DrainReader(obj)
	}
	FreeRecv(obj)
}
