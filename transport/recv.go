// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xoshiro256"
	"github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v3"
)

// private types
type (
	iterator struct {
		trname    string
		body      io.Reader
		fbuf      *fixedBuffer // when extraBuffering == true
		headerBuf []byte
	}
	objReader struct {
		body io.Reader
		off  int64
		fbuf *fixedBuffer // ditto
		hdr  ObjHdr
	}
	handler struct {
		trname      string
		rxObj       ReceiveObj
		rxMsg       ReceiveMsg
		sessions    sync.Map // map[uint64]*Stats
		oldSessions sync.Map // map[uint64]time.Time
		hkName      string   // house-keeping name
		mem         *memsys.MMSA
	}
	fixedBuffer struct {
		slab *memsys.Slab
		buf  []byte
		roff int
		woff int
	}
)

const (
	cleanupInterval = time.Minute * 10

	// compressed streams perf tunables
	slabBufferSize = memsys.DefaultBufSize
	extraBuffering = false
)

//
// globals
//

var (
	mu       *sync.RWMutex
	handlers map[string]*handler // by trname
	verbose  bool
)

func init() {
	mu = &sync.RWMutex{}
	handlers = make(map[string]*handler, 16)
	verbose = bool(glog.FastV(4, glog.SmoduleTransport))
}

// main Rx objects
func RxAnyStream(w http.ResponseWriter, r *http.Request) {
	var (
		reader    io.Reader = r.Body
		lz4Reader *lz4.Reader
		fbuf      *fixedBuffer
		trname    = path.Base(r.URL.Path)
	)
	mu.RLock()
	h, ok := handlers[trname]
	if !ok {
		mu.RUnlock()
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("transport endpoint %s is unknown", trname))
		return
	}
	mu.RUnlock()
	// compression
	if compressionType := r.Header.Get(cmn.HeaderCompress); compressionType != "" {
		cmn.Assert(compressionType == cmn.LZ4Compression)
		lz4Reader = lz4.NewReader(r.Body)
		reader = lz4Reader
		if extraBuffering {
			fbuf = newFixedBuffer(h.mem)
		}
	}
	// session
	sessID, err := strconv.ParseInt(r.Header.Get(cmn.HeaderSessID), 10, 64)
	if err != nil || sessID == 0 {
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("%s[:%d]: invalid session ID, err %v", trname, sessID, err))
		return
	}
	uid := uniqueID(r, sessID)
	statsif, loaded := h.sessions.LoadOrStore(uid, &Stats{})
	if !loaded && debug.Enabled {
		xxh, id := UID2SessID(uid)
		debug.Assert(id == uint64(sessID))
		debug.Infof("%s[%d:%d]: start-of-stream from %s", trname, xxh, sessID, r.RemoteAddr) // r.RemoteAddr => xxh
	}
	stats := statsif.(*Stats)

	// Rx loop
	it := &iterator{trname: trname, body: reader, fbuf: fbuf, headerBuf: make([]byte, maxHeaderSize)}
	for {
		var (
			obj *objReader
			msg Msg
		)
		hlen, isObj, err := it.nextProtoHdr()
		if err != nil {
			goto rerr
		}
		_ = stats.Offset.Add(int64(hlen + cmn.SizeofI64*2)) // to account for proto header
		if isObj {
			obj, err = it.nextObj(hlen)
			er := err
			if er == io.EOF {
				er = nil
			}
			if obj != nil {
				h.rxObj(w, obj.hdr, obj, er)
				hdr := &obj.hdr
				if hdr.ObjAttrs.Size == obj.off {
					var (
						num = stats.Num.Inc()
						_   = stats.Size.Add(hdr.ObjAttrs.Size)
						off = stats.Offset.Add(hdr.ObjAttrs.Size)
					)
					if verbose {
						xxh, _ := UID2SessID(uid)
						glog.Infof("%s[%d:%d] %s, num %d, off %d", trname, xxh, sessID, obj, num, off)
					}
					continue
				}
				xxh, _ := UID2SessID(uid)
				num := stats.Num.Load()
				err = fmt.Errorf("%s[%d:%d]: %v, num %d, off %d != %s",
					trname, xxh, sessID, err, num, obj.off, obj)
			}
		} else {
			msg, err = it.nextMsg(hlen)
			er := err
			if er == io.EOF {
				er = nil
			}
			h.rxMsg(w, msg, er)
		}
	rerr:
		if err != nil {
			h.oldSessions.Store(uid, time.Now())
			if err != io.EOF {
				h.rxObj(w, ObjHdr{}, nil, err)
				cmn.InvalidHandlerDetailed(w, r, err.Error())
			}
			if lz4Reader != nil {
				lz4Reader.Reset(nil)
			}
			if fbuf != nil {
				fbuf.Free()
			}
			return
		}
	}
}

/////////////
// handler //
/////////////

func (h *handler) handle() error {
	mu.Lock()
	if _, ok := handlers[h.trname]; ok {
		mu.Unlock()
		return fmt.Errorf("duplicate trname %q: re-registering is not permitted", h.trname)
	}
	handlers[h.trname] = h
	mu.Unlock()
	hk.Reg(h.hkName, h.cleanupOldSessions)
	return nil
}

func (h *handler) cleanupOldSessions() time.Duration {
	now := time.Now()
	f := func(key, value interface{}) bool {
		uid := key.(uint64)
		timeClosed := value.(time.Time)
		if now.Sub(timeClosed) > cleanupInterval {
			h.oldSessions.Delete(uid)
			h.sessions.Delete(uid)
		}
		return true
	}
	h.oldSessions.Range(f)
	return cleanupInterval
}

//////////////
// iterator //
//////////////

func (it *iterator) Read(p []byte) (n int, err error) {
	if it.fbuf == nil {
		return it.body.Read(p)
	}
	if it.fbuf.Len() != 0 {
		goto read
	}
nextchunk:
	it.fbuf.Reset()
	n, err = it.body.Read(it.fbuf.Bytes())
	it.fbuf.Written(n)
read:
	n, _ = it.fbuf.Read(p)
	if err == nil && n < len(p) {
		debug.Assert(it.fbuf.Len() == 0)
		p = p[n:]
		goto nextchunk
	}
	return
}

// nextProtoHdr receives and handles 16 bytes of the protocol header (not to confuse with transport.Obj.Hdr)
// returns hlen, which is header length - for transport.Obj, and message length - for transport.Msg
func (it *iterator) nextProtoHdr() (hlen int, isObj bool, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:cmn.SizeofI64*2])
	if n < cmn.SizeofI64*2 {
		if err == nil {
			err = fmt.Errorf("sbrk %s: failed to receive proto hdr (n=%d)", it.trname, n)
		}
		return
	}
	// extract and validate hlen
	_, hl64ex := extUint64(0, it.headerBuf)
	isObj = hl64ex&msgMask == 0
	hl64 := int64(hl64ex & ^msgMask)
	hlen = int(hl64)
	if hlen > len(it.headerBuf) {
		err = fmt.Errorf("sbrk %s: hlen %d exceeds buf %d", it.trname, hlen, len(it.headerBuf))
		return
	}
	// validate checksum
	_, checksum := extUint64(0, it.headerBuf[cmn.SizeofI64:])
	chc := xoshiro256.Hash(uint64(hl64))
	if checksum != chc {
		err = fmt.Errorf("sbrk %s: bad checksum %x != %x (hlen=%d)", it.trname, checksum, chc, hlen)
	}
	return
}

func (it *iterator) nextObj(hlen int) (obj *objReader, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:hlen])
	if n < hlen {
		if err == nil {
			err = fmt.Errorf("sbrk %s: failed to receive obj hdr (%d < %d)", it.trname, n, hlen)
		}
		return
	}
	hdr := ExtObjHeader(it.headerBuf, hlen)
	if hdr.IsLast() {
		err = io.EOF
		return
	}
	obj = &objReader{body: it.body, fbuf: it.fbuf, hdr: hdr}
	return
}

func (it *iterator) nextMsg(hlen int) (msg Msg, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:hlen])
	if n < hlen {
		if err == nil {
			err = fmt.Errorf("sbrk %s: failed to receive msg (%d < %d)", it.trname, n, hlen)
		}
		return
	}
	debug.Assertf(n == hlen, "%d != %d", n, hlen)
	msg = ExtMsg(it.headerBuf, hlen)
	if msg.IsLast() {
		err = io.EOF
	}
	return
}

///////////////
// objReader //
///////////////

func (obj *objReader) Read(b []byte) (n int, err error) {
	rem := obj.hdr.ObjAttrs.Size - obj.off
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	if obj.fbuf != nil && obj.fbuf.Len() > 0 {
		n, _ = obj.fbuf.Read(b)
		if n < len(b) {
			b = b[n:]
			nr, er := obj.body.Read(b)
			n += nr
			err = er
		}
	} else {
		n, err = obj.body.Read(b)
	}
	obj.off += int64(n)
	switch err {
	case nil:
		if obj.off >= obj.hdr.ObjAttrs.Size {
			err = io.EOF
			debug.Assert(obj.off == obj.hdr.ObjAttrs.Size)
		}
	case io.EOF:
		if obj.off != obj.hdr.ObjAttrs.Size {
			glog.Errorf("actual off: %d, expected: %d", obj.off, obj.hdr.ObjAttrs.Size)
		}
	default:
		glog.Errorf("err %v\n", err) // canceled?
	}
	return
}

func (obj *objReader) String() string {
	return fmt.Sprintf("%s/%s(size=%d)", obj.hdr.Bck, obj.hdr.ObjName, obj.hdr.ObjAttrs.Size)
}

/////////////////
// fixedBuffer //
/////////////////

func newFixedBuffer(mem *memsys.MMSA) (bb *fixedBuffer) {
	if mem == nil {
		mem = memsys.DefaultPageMM()
	}
	buf, slab := mem.Alloc(slabBufferSize)
	return &fixedBuffer{slab: slab, buf: buf}
}

func (bb *fixedBuffer) Read(p []byte) (n int, err error) {
	n = copy(p, bb.buf[bb.roff:bb.woff])
	bb.roff += n
	return
}

func (bb *fixedBuffer) Bytes() []byte { return bb.buf }
func (bb *fixedBuffer) Len() int      { return bb.woff - bb.roff }
func (bb *fixedBuffer) Reset()        { bb.roff, bb.woff = 0, 0 }
func (bb *fixedBuffer) Written(n int) { bb.woff += n }
func (bb *fixedBuffer) Free()         { bb.slab.Free(bb.buf) }

/////////////////////////////////////////
// obj-header/message de-serialization //
/////////////////////////////////////////

func ExtObjHeader(body []byte, hlen int) (hdr ObjHdr) {
	var off int
	off, hdr.Bck.Name = extString(0, body)
	off, hdr.ObjName = extString(off, body)
	off, hdr.Bck.Provider = extString(off, body)
	off, hdr.Bck.Ns.Name = extString(off, body)
	off, hdr.Bck.Ns.UUID = extString(off, body)
	off, hdr.Opaque = extByte(off, body)
	off, hdr.ObjAttrs = extAttrs(off, body)
	debug.Assertf(off == hlen, "off %d, hlen %d", off, hlen)
	return
}

func ExtMsg(body []byte, hlen int) (msg Msg) {
	var off int
	off, msg.Flags = extInt64(0, body)
	off, msg.Body = extByte(off, body)
	debug.Assertf(off == hlen, "off %d, hlen %d", off, hlen)
	return
}

func extString(off int, from []byte) (int, string) {
	off, bt := extByte(off, from)
	return off, string(bt)
}

func extByte(off int, from []byte) (int, []byte) {
	l := int(binary.BigEndian.Uint64(from[off:]))
	off += cmn.SizeofI64
	return off + l, from[off : off+l]
}

func extInt64(off int, from []byte) (int, int64) {
	off, val := extUint64(off, from)
	return off, int64(val)
}

func extUint64(off int, from []byte) (int, uint64) {
	size := binary.BigEndian.Uint64(from[off:])
	off += cmn.SizeofI64
	return off, size
}

func extAttrs(off int, from []byte) (n int, attr ObjectAttrs) {
	off, attr.Size = extInt64(off, from)
	off, attr.Atime = extInt64(off, from)
	off, attr.CksumType = extString(off, from)
	off, attr.CksumValue = extString(off, from)
	off, attr.Version = extString(off, from)
	return off, attr
}

//
// session ID <=> unique ID
//

func uniqueID(r *http.Request, sessID int64) uint64 {
	x := xxhash.ChecksumString64S(r.RemoteAddr, cmn.MLCG32)
	return (x&math.MaxUint32)<<32 | uint64(sessID)
}

func UID2SessID(uid uint64) (xxh, sessID uint64) {
	xxh, sessID = uid>>32, uid&math.MaxUint32
	return
}
