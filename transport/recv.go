// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

const sessionIsOld = time.Hour

// private types
type (
	rxStats interface {
		addOff(int64)
		incNum()
	}
	iterator struct {
		body    io.Reader
		handler handler
		pdu     *rpdu
		stats   rxStats
		hbuf    []byte
	}
	objReader struct {
		body   io.Reader
		pdu    *rpdu
		loghdr string
		hdr    ObjHdr
		off    int64
	}

	handler interface {
		recv(hdr *ObjHdr, objReader io.Reader, err error) error // RecvObj
		stats(*http.Request, string) (rxStats, uint64, string)
		unreg()
		addOld(uint64)
		getStats() RxStats
	}
	hdl struct {
		rxObj  RecvObj
		trname string
		now    int64
	}
	hdlExtra struct {
		sessions    sync.Map
		oldSessions sync.Map
		hkName      string
		hdl
	}
)

// interface guard
var (
	_ handler = (*hdl)(nil)
	_ handler = (*hdlExtra)(nil)
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
		config             = cmn.GCO.Get()
		stats, uid, loghdr = h.stats(r, trname)
		it                 = &iterator{handler: h, body: reader, stats: stats}
	)
	debug.Assert(config.Transport.IdleTeardown > 0, "invalid config ", config.Transport)
	it.hbuf, _ = mm.AllocSize(_sizeHdr(config, 0))

	// receive loop (until eof or error)
	err = it.rxloop(uid, loghdr, mm)

	// cleanup
	if lz4Reader != nil {
		lz4Reader.Reset(nil)
	}
	if it.pdu != nil {
		it.pdu.free(mm)
	}
	mm.Free(it.hbuf)

	if !cos.IsEOF(err) {
		cmn.WriteErr(w, r, err)
	}
}

////////////////
// Rx handler //
////////////////

// begin t2t session
func (h *hdl) stats(r *http.Request, trname string) (rxStats, uint64, string) {
	debug.Assertf(h.trname == trname, "%q vs %q", h.trname, trname)
	sid := r.Header.Get(apc.HdrSessID)
	loghdr := h.trname + "[" + r.RemoteAddr + ":" + sid + "]"
	return nopRxStats{}, 0, loghdr
}

// ditto, with Rx stats
func (h *hdlExtra) stats(r *http.Request, trname string) (rxStats, uint64, string) {
	debug.Assertf(h.trname == trname, "%q vs %q", h.trname, trname)
	sid := r.Header.Get(apc.HdrSessID)

	sessID, err := strconv.ParseInt(sid, 10, 64)
	if err != nil || sessID == 0 {
		err = fmt.Errorf("%s[:%q]: invalid session ID, err %w", h.trname, sid, err)
		cos.AssertNoErr(err)
	}

	// yet another id to index optional h.sessions & h.oldSessions sync.Maps
	uid := uniqueID(r, sessID)
	statsif, _ := h.sessions.LoadOrStore(uid, &Stats{})

	xxh, _ := UID2SessID(uid)
	loghdr := fmt.Sprintf("%s[%d:%d]", h.trname, xxh, sessID)
	if cmn.Rom.V(5, cos.ModTransport) {
		nlog.Infoln(loghdr, "start-of-stream from", r.RemoteAddr)
	}
	return statsif.(rxStats), uid, loghdr
}

func (*hdl) unreg()        {}
func (h *hdlExtra) unreg() { hk.Unreg(h.hkName + hk.NameSuffix) }

func (*hdl) addOld(uint64)            {}
func (h *hdlExtra) addOld(uid uint64) { h.oldSessions.Store(uid, mono.NanoTime()) }

func (h *hdlExtra) cleanup(now int64) time.Duration {
	h.now = now
	h.oldSessions.Range(h.cl)
	return sessionIsOld
}

func (h *hdlExtra) cl(key, value any) bool {
	timeClosed := value.(int64)
	if time.Duration(h.now-timeClosed) > sessionIsOld {
		uid := key.(uint64)
		h.oldSessions.Delete(uid)
		h.sessions.Delete(uid)
	}
	return true
}

func (h *hdl) recv(hdr *ObjHdr, objReader io.Reader, err error) error {
	return h.rxObj(hdr, objReader, err)
}

func (*hdl) getStats() RxStats { return nil }

func (h *hdlExtra) getStats() (s RxStats) {
	s = make(RxStats, 4)
	h.sessions.Range(s.f)
	return s
}

func (s RxStats) f(key, value any) bool {
	out := &Stats{}
	uid := key.(uint64)
	in := value.(*Stats)
	out.Num.Store(in.Num.Load())       // via rxStats.incNum
	out.Offset.Store(in.Offset.Load()) // via rxStats.addOff
	s[uid] = out
	return true
}

//////////////////////////////////
// next(obj, msg, pdu) iterator //
//////////////////////////////////

func (it *iterator) Read(p []byte) (n int, err error) { return it.body.Read(p) }

func (it *iterator) rxloop(uid uint64, loghdr string, mm *memsys.MMSA) (err error) {
	for err == nil {
		var (
			flags uint64
			hlen  int
		)
		hlen, flags, err = it.nextProtoHdr(loghdr)
		if err != nil {
			break
		}
		if hlen > cap(it.hbuf) {
			if hlen > cmn.MaxTransportHeader {
				err = fmt.Errorf("sbr1 %s: transport header %d exceeds maximum %d", loghdr, hlen, cmn.MaxTransportHeader)
				break
			}
			// grow
			nlog.Warningf("%s: header length %d exceeds the current buffer %d", loghdr, hlen, cap(it.hbuf))
			mm.Free(it.hbuf)
			it.hbuf, _ = mm.AllocSize(min(int64(hlen)<<1, cmn.MaxTransportHeader))
		}

		it.stats.addOff(int64(hlen + sizeProtoHdr))
		debug.Assert(flags&msgFl == 0) //  messaging: not used, removed
		if flags&pduStreamFl != 0 {
			if it.pdu == nil {
				pbuf, _ := mm.AllocSize(maxSizePDU)
				it.pdu = newRecvPDU(it.body, pbuf)
			} else {
				it.pdu.reset()
			}
		}
		err = it.rxObj(loghdr, hlen)
	}

	it.handler.addOld(uid)
	return err
}

func (it *iterator) rxObj(loghdr string, hlen int) (err error) {
	var (
		obj *objReader
		h   = it.handler
	)
	obj, err = it.nextObj(loghdr, hlen)
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
			it.stats.incNum()                   // 1. this stream stats
			g.tstats.Inc(cos.StreamsInObjCount) // 2. stats/target_stats.go

			if size >= 0 {
				g.tstats.Add(cos.StreamsInObjSize, size)
			} else {
				debug.Assert(size == SizeUnknown)
				g.tstats.Add(cos.StreamsInObjSize, obj.off-off)
			}
		}
	} else if err != nil && err != io.EOF {
		if errCb := h.recv(&ObjHdr{}, nil, err); errCb != nil {
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
func (it *iterator) nextProtoHdr(loghdr string) (int, uint64, error) {
	n, err := it.Read(it.hbuf[:sizeProtoHdr])
	if n < sizeProtoHdr {
		if err == nil {
			err = fmt.Errorf("sbr3 %s: failed to receive proto hdr (n=%d)", loghdr, n)
		}
		return 0, 0, err
	}
	// extract and validate hlen
	return extProtoHdr(it.hbuf, loghdr)
}

func (it *iterator) nextObj(loghdr string, hlen int) (*objReader, error) {
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
			return nil, fmt.Errorf("sbr4 %s: failed to receive obj hdr (%d < %d)", loghdr, n, hlen)
		}
	}
	hdr := ExtObjHeader(it.hbuf, hlen)
	if hdr.isFin() {
		return nil, io.EOF
	}

	obj := allocRecv()
	obj.body, obj.hdr, obj.loghdr = it.body, hdr, loghdr
	return obj, nil
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
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	n, err = obj.body.Read(b)
	obj.off += int64(n) // NOTE: `GORACE` complaining here can be safely ignored
	switch err {
	case nil:
		if obj.off >= obj.Size() {
			err = io.EOF
		}
	case io.EOF:
		if obj.off != obj.Size() {
			err = fmt.Errorf("sbr6 %s: premature eof %d != %s, err %w", obj.loghdr, obj.off, obj, err)
		}
	default:
		err = fmt.Errorf("sbr7 %s: off %d, obj %s, err %w", obj.loghdr, obj.off, obj, err)
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
		err = pdu.readHdr(obj.loghdr)
		if err != nil {
			return 0, err
		}
	}
	for !pdu.done {
		if _, err = pdu.readFrom(); err != nil && err != io.EOF {
			err = fmt.Errorf("sbr8 %s: failed to receive PDU, err %w, obj %s", obj.loghdr, err, obj)
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
				err = fmt.Errorf("sbr9 %s: off %d != %s", obj.loghdr, obj.off, obj)
				nlog.Warningln(err)
			}
		} else {
			pdu.reset()
		}
	}
	return n, err
}

//
// session ID <=> unique ID
//

func uniqueID(r *http.Request, sessID int64) uint64 {
	hash := onexxh.Checksum64S(cos.UnsafeB(r.RemoteAddr), cos.MLCG32)
	return (hash&math.MaxUint32)<<32 | uint64(sessID)
}

func UID2SessID(uid uint64) (xxh, sessID uint64) {
	xxh, sessID = uid>>32, uid&math.MaxUint32
	return xxh, sessID
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
