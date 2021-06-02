// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v3"
)

// private types
type (
	iterator struct {
		trname    string
		body      io.Reader
		headerBuf []byte
		pdu       *rpdu
	}
	objReader struct {
		body   io.Reader
		off    int64
		hdr    ObjHdr
		pdu    *rpdu
		loghdr string
	}
	handler struct {
		trname      string
		rxObj       ReceiveObj
		rxMsg       ReceiveMsg
		sessions    sync.Map // map[uint64]*Stats
		oldSessions sync.Map // map[uint64]time.Time
		hkName      string   // house-keeping name
		mm          *memsys.MMSA
	}
)

const cleanupInterval = time.Minute * 10

var (
	nextSID  atomic.Int64        // next unique session ID
	handlers map[string]*handler // by trname
	mu       *sync.RWMutex       // ptotect handlers
	verbose  bool
)

func init() {
	nextSID.Store(100)
	handlers = make(map[string]*handler, 16)
	mu = &sync.RWMutex{}
	verbose = bool(glog.FastV(4, glog.SmoduleTransport))
}

// main Rx objects
func RxAnyStream(w http.ResponseWriter, r *http.Request) {
	var (
		reader    io.Reader = r.Body
		lz4Reader *lz4.Reader
		trname    = path.Base(r.URL.Path)
	)
	mu.RLock()
	h, ok := handlers[trname]
	if !ok {
		mu.RUnlock()
		cmn.WriteErr(w, r, fmt.Errorf(cmn.FmtErrUnknown, "transport", "endpoint", trname))
		return
	}
	mu.RUnlock()
	// compression
	if compressionType := r.Header.Get(cmn.HdrCompress); compressionType != "" {
		debug.Assert(compressionType == cmn.LZ4Compression)
		lz4Reader = lz4.NewReader(r.Body)
		reader = lz4Reader
	}
	// session
	sessID, err := strconv.ParseInt(r.Header.Get(cmn.HdrSessID), 10, 64)
	if err != nil || sessID == 0 {
		cmn.WriteErr(w, r, fmt.Errorf("%s[:%d]: invalid session ID, err %v", trname, sessID, err))
		return
	}
	uid := uniqueID(r, sessID)
	statsif, loaded := h.sessions.LoadOrStore(uid, &Stats{})
	xxh, id := UID2SessID(uid)
	loghdr := fmt.Sprintf("%s[%d:%d]", trname, xxh, sessID)
	debug.Func(func() {
		if !loaded {
			debug.Assert(id == uint64(sessID))
			debug.Infof("%s: start-of-stream from %s", loghdr, r.RemoteAddr)
		}
	})
	stats := statsif.(*Stats)

	// Rx loop
	it := &iterator{trname: trname, body: reader, headerBuf: make([]byte, maxHeaderSize)}
	defer func() {
		if it.pdu != nil {
			it.pdu.free(h.mm)
		}
	}()
	for {
		var (
			obj              *objReader
			msg              Msg
			hlen, flags, err = it.nextProtoHdr(loghdr)
		)
		if err != nil {
			goto rerr
		}
		if lh := len(it.headerBuf); hlen > lh {
			err = fmt.Errorf("sbr1 %s: hlen %d exceeds buflen=%d", loghdr, hlen, lh)
			goto rerr
		}
		_ = stats.Offset.Add(int64(hlen + sizeProtoHdr))
		if flags&msgFlag == 0 {
			obj, err = it.nextObj(loghdr, hlen)
			if obj != nil {
				unsized := obj.IsUnsized()
				if flags&firstPDU != 0 && !obj.hdr.IsHeaderOnly() {
					if it.pdu == nil {
						buf, _ := h.mm.Alloc(MaxSizePDU)
						it.pdu = newRecvPDU(it.body, buf)
					}
					obj.pdu = it.pdu
					obj.pdu.reset()
				}
				// err => err; EOF => (unsized => EOF, otherwise => nil)
				h.rxObj(obj.hdr, obj, eofOK(err, unsized))
				stats.Num.Inc()
				// NOTE: `GORACE` may erroneously trigger at this point vs. objReader.Read
				// below incrementing the offset - disabling `recvPool` makes it go away
				stats.Size.Add(obj.off)
				if eofOK(err) == nil {
					continue
				}
			} else if err != nil && err != io.EOF {
				h.rxObj(ObjHdr{}, nil, err)
			}
		} else {
			msg, err = it.nextMsg(loghdr, hlen)
			if err == nil {
				h.rxMsg(msg, nil)
			} else if err != io.EOF {
				h.rxMsg(Msg{}, err)
			}
		}
	rerr:
		if err == nil {
			continue
		}
		h.oldSessions.Store(uid, time.Now())
		if err != io.EOF {
			cmn.WriteErr(w, r, err)
		}
		if lz4Reader != nil {
			lz4Reader.Reset(nil)
		}
		return
	}
}

func eofOK(err error, unsized ...bool) error {
	if len(unsized) > 0 && unsized[0] {
		if err == nil {
			return io.EOF
		}
		return err
	}
	if err == io.EOF {
		return nil
	}
	return err
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

func (it *iterator) Read(p []byte) (n int, err error) { return it.body.Read(p) }

// nextProtoHdr receives and handles 16 bytes of the protocol header (not to confuse with transport.Obj.Hdr)
// returns hlen, which is header length - for transport.Obj, and message length - for transport.Msg
func (it *iterator) nextProtoHdr(loghdr string) (hlen int, flags uint64, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:sizeProtoHdr])
	if n < sizeProtoHdr {
		if err == nil {
			err = fmt.Errorf("sbr3 %s: failed to receive proto hdr (n=%d)", loghdr, n)
		}
		return
	}
	// extract and validate hlen
	hlen, flags, err = extProtoHdr(it.headerBuf, loghdr)
	return
}

func (it *iterator) nextObj(loghdr string, hlen int) (obj *objReader, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:hlen])
	if n < hlen {
		if err == nil {
			err = fmt.Errorf("sbr4 %s: failed to receive obj hdr (%d < %d)", loghdr, n, hlen)
		}
		return
	}
	hdr := ExtObjHeader(it.headerBuf, hlen)
	if hdr.IsLast() {
		err = io.EOF
		return
	}
	obj = allocRecv()
	obj.body, obj.hdr, obj.loghdr = it.body, hdr, loghdr
	return
}

func (it *iterator) nextMsg(loghdr string, hlen int) (msg Msg, err error) {
	var n int
	n, err = it.Read(it.headerBuf[:hlen])
	if n < hlen {
		if err == nil {
			err = fmt.Errorf("sbr5 %s: failed to receive msg (%d < %d)", loghdr, n, hlen)
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
	if obj.pdu != nil {
		return obj.readPDU(b)
	}
	debug.Assert(obj.Size() >= 0)
	rem := obj.Size() - obj.off
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	n, err = obj.body.Read(b)
	obj.off += int64(n)
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
	return
}

func (obj *objReader) String() string {
	return fmt.Sprintf("%s/%s(size=%d)", obj.hdr.Bck, obj.hdr.ObjName, obj.Size())
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
			return
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
		return
	}
	if pdu.rlength() == 0 {
		if pdu.last {
			err = io.EOF
			if obj.IsUnsized() {
				obj.hdr.ObjAttrs.Size = obj.off
			} else if obj.Size() != obj.off {
				glog.Errorf("sbr9 %s: off %d != %s", obj.loghdr, obj.off, obj)
			}
		} else {
			pdu.reset()
		}
	}
	return
}

//
// session ID <=> unique ID
//

func uniqueID(r *http.Request, sessID int64) uint64 {
	x := xxhash.ChecksumString64S(r.RemoteAddr, cos.MLCG32)
	return (x&math.MaxUint32)<<32 | uint64(sessID)
}

func UID2SessID(uid uint64) (xxh, sessID uint64) {
	xxh, sessID = uid>>32, uid&math.MaxUint32
	return
}
