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
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
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
	}
	objReader struct {
		body io.Reader
		off  int64
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
		cmn.InvalidHandlerDetailed(w, r, fmt.Sprintf("transport endpoint %s is unknown", trname))
		return
	}
	mu.RUnlock()
	// compression
	if compressionType := r.Header.Get(cmn.HeaderCompress); compressionType != "" {
		cmn.Assert(compressionType == cmn.LZ4Compression)
		lz4Reader = lz4.NewReader(r.Body)
		reader = lz4Reader
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
	it := &iterator{trname: trname, body: reader, headerBuf: make([]byte, maxHeaderSize)}
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
			if obj != nil {
				h.rxObj(w, obj.hdr, obj, eofOK(err))
				hdr := &obj.hdr
				objSize := hdr.ObjAttrs.Size
				if hdr.IsUnsized() && err == io.EOF {
					objSize = obj.off
				}
				if objSize == obj.off {
					var (
						num = stats.Num.Inc()
						_   = stats.Size.Add(objSize)
						off = stats.Offset.Add(objSize)
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
			} else if err != nil && err != io.EOF {
				h.rxObj(w, ObjHdr{}, nil, err)
			}
		} else {
			msg, err = it.nextMsg(hlen)
			if err == nil {
				h.rxMsg(w, msg, nil)
			} else if err != io.EOF {
				h.rxMsg(w, Msg{}, err)
			}
		}
	rerr:
		if err == nil {
			continue
		}
		h.oldSessions.Store(uid, time.Now())
		if err != io.EOF {
			cmn.InvalidHandlerDetailed(w, r, err.Error())
		}
		if lz4Reader != nil {
			lz4Reader.Reset(nil)
		}
		return
	}
}

func eofOK(err error) error {
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
	hlen, isObj, err = extProtoHdr(it.headerBuf, it.trname)
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
	if obj = allocRecv(); obj != nil {
		*obj = objReader{body: it.body, hdr: hdr}
	} else {
		obj = &objReader{body: it.body, hdr: hdr}
	}
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
	if obj.hdr.IsUnsized() {
		n, err = obj.body.Read(b)
		obj.off += int64(n)
		return
	}
	objSize := obj.hdr.ObjAttrs.Size
	rem := objSize - obj.off
	if rem < int64(len(b)) {
		b = b[:int(rem)]
	}
	n, err = obj.body.Read(b)
	obj.off += int64(n)
	switch err {
	case nil:
		if obj.off >= objSize {
			err = io.EOF
			debug.Assert(obj.off == objSize)
		}
	case io.EOF:
		if obj.off != objSize {
			glog.Errorf("actual off: %d, expected: %d", obj.off, objSize)
		}
	default:
		glog.Errorf("err %v\n", err) // canceled?
	}
	return
}

func (obj *objReader) String() string {
	return fmt.Sprintf("%s/%s(size=%d)", obj.hdr.Bck, obj.hdr.ObjName, obj.hdr.ObjAttrs.Size)
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
