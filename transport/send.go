/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package transport provides L5.5 transport over http for intra-DFC or DFC-to-DFC rebalancing,
// replication, and more.
//
// * On the wire, each transmitted object will have the layout:
//
//   [header length] [header fields including object size}] [object bytes])
//
// * The size must be known upfront, which is the current limitation.
//
// * A stream (the Stream type below) carries a sequence of objects of arbitrary length
//   and overall looks as follows:
//
//   object1 = ([header1], [data1]) object2 = ([header2], [data2]), etc.
//
// * Stream termination is denoted by a special marker in the data-size field of the header:
//
//   header = [object size=^uint64(0) >> 1 (7fffffffffffffff)]
//
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/common"
)

const (
	MaxHeaderSize  = 1024
	lastMarker     = common.MaxInt64
	defaultIdleOut = time.Second
	wakeupOut      = time.Millisecond * 100
	sizeofI64      = int(unsafe.Sizeof(uint64(0)))
)

// API: types
type (
	Stream struct {
		// user-defined
		client        *http.Client // http client thios send-stream will use
		toURL, trname string       // http endpoint
		// internals
		sid       int64         // stream session ID
		lid       string        // log prefix
		workCh    chan obj      // next object to stream
		lastCh    chan struct{} // end of stream
		stopCh    chan struct{} // stop/abort stream
		maxheader []byte        // max header buffer
		header    []byte        // object header - slice of the maxheader with bucket/objname, etc. fields
		time      struct {
			idleOut time.Duration // inter-object timeout: when triggers, causes recycling of the underlying http request
			idle    *time.Timer
			wakeup  *time.Timer
			expired int64
		}
		wg      sync.WaitGroup
		sendoff sendoff
		stats   struct {
			numtot, numcur   int64 // number of sent objects: total and current
			sizetot, sizecur int64 // transferred size in bytes: ditto
		}
	}
	Header struct {
		Bucket, Objname string // uname at the destination
		Opaque          []byte // custom control (optional)
		Dsize           int64  // size of the object (size=0 (unknown) not supported yet)
	}
)

// internal
type (
	obj struct {
		hdr    Header
		reader io.ReadCloser // reader, to read the object, and close when done
	}
	sendoff struct {
		obj obj
		// in progress
		off int64
		dod int64
	}
)

//
// API: methods
//
func NewStream(client *http.Client, toURL string, idleTimeout ...time.Duration) (s *Stream) {
	if client == nil {
		glog.Errorln("nil client")
		return
	}
	u, err := url.Parse(toURL)
	if err != nil {
		glog.Errorf("Failed to parse %s: %v", toURL, err)
		return
	}
	s = &Stream{client: client, toURL: toURL}

	s.time.idleOut = defaultIdleOut
	if len(idleTimeout) > 0 {
		s.time.idleOut = idleTimeout[0]
	}
	s.sid = time.Now().UnixNano() & 0xfff // FIXME: xorshift(daemon-id, time)
	s.trname = path.Base(u.Path)
	s.lid = fmt.Sprintf("%s[%d]:", s.trname, s.sid)

	s.workCh = make(chan obj, 128)
	s.lastCh = make(chan struct{}, 1)
	s.stopCh = make(chan struct{}, 1)
	s.maxheader = make([]byte, MaxHeaderSize)
	s.time.idle = time.NewTimer(s.time.idleOut)
	s.time.idle.Stop()
	s.time.wakeup = time.NewTimer(wakeupOut)
	s.time.wakeup.Stop()

	s.wg.Add(1)
	go s.doTx()
	return
}

func (s *Stream) SendAsync(hdr Header, reader io.ReadCloser) {
	if glog.V(4) {
		glog.Infof("%s send-async [%+v]", s.lid, hdr)
	}
	s.time.idle.Stop()
	if atomic.CompareAndSwapInt64(&s.time.expired, lastMarker, 0) {
		glog.Infof("%s wake-up", s.lid)
	}
	s.workCh <- obj{hdr, reader}
	return
}

func (s *Stream) Fin() {
	s.workCh <- obj{hdr: Header{Dsize: lastMarker}}
	s.wg.Wait() // synchronous termination
}

func (s *Stream) Stop() {
	s.stopCh <- struct{}{}
}

func (hdr *Header) IsLast() bool { return hdr.Dsize == lastMarker }

//
// internal methods
//
func (s *Stream) doTx() {
	defer s.wg.Done()
outer:
	for {
		var (
			request  *http.Request
			response *http.Response
			err      error
		)
		defer func() {
			if err != nil {
				glog.Errorf("%s exiting with err %v", s.lid, err)
			}
		}()

		if request, err = http.NewRequest(http.MethodPut, s.toURL, s); err != nil {
			break
		}
		s.stats.numcur, s.stats.sizecur = 0, 0
		glog.Infof("%s Do", s.lid)
		response, err = s.client.Do(request)
		if err == nil {
			glog.Infof("%s Done", s.lid)
		} else {
			glog.Infof("%s Done, err %v", s.lid, err)
		}
		if err != nil {
			break
		}
		ioutil.ReadAll(response.Body)
		response.Body.Close()
		//
		// upon timeout, initiate HTTP/TCP session *upon arrival* of the first object, and not earlier
		//
		s.time.wakeup.Reset(wakeupOut)
	inner:
		for {
			select {
			case <-s.lastCh:
				glog.Infof("%s end-of-stream", s.lid)
				s.time.wakeup.Stop()
				break outer
			case <-s.stopCh:
				glog.Infof("%s stopped", s.lid)
				s.time.wakeup.Stop()
				break outer
			case <-s.time.wakeup.C:
				if atomic.LoadInt64(&s.time.expired) == lastMarker {
					s.time.wakeup.Reset(wakeupOut)
				} else {
					break inner
				}
			}
		}
	}
	// this stream is done - terminated: closing the work channel on purpose
	close(s.workCh)
	close(s.lastCh)
	close(s.stopCh)
}

// as io.Reader
func (s *Stream) Read(b []byte) (n int, err error) {
	// current object
	if s.sendoff.obj.reader != nil {
		if s.sendoff.dod != 0 {
			return s.sendData(b)
		}
		return s.sendHdr(b)
	}
	select {
	// next object
	case s.sendoff.obj = <-s.workCh:
		s.time.idle.Stop()
		l := s.insHeader(s.sendoff.obj.hdr)
		s.header = s.maxheader[:l]
		return s.sendHdr(b)
	// control stream lifesycle on object boundaries
	case <-s.time.idle.C:
		err = io.EOF
		glog.Warningf("%s timed out (%d/%d)", s.lid, s.stats.numcur, s.stats.numtot)
		atomic.CompareAndSwapInt64(&s.time.expired, 0, lastMarker)
		return
	case <-s.stopCh:
		glog.Infof("%s stopped (%d/%d)", s.lid, s.stats.numcur, s.stats.numtot)
		s.stopCh <- struct{}{}
		err = io.EOF
		return
	}
}

func (s *Stream) sendHdr(b []byte) (n int, err error) {
	n = copy(b, s.header[s.sendoff.off:])
	s.sendoff.off += int64(n)
	if s.sendoff.off >= int64(len(s.header)) {
		if debug {
			common.Assert(s.sendoff.off == int64(len(s.header)))
		}
		if glog.V(4) {
			glog.Infof("%s hlen=%d (%d/%d)", s.lid, s.sendoff.off, s.stats.numcur, s.stats.numtot)
		}
		s.sendoff.dod = s.sendoff.off
		s.sendoff.off = 0

		obj := &s.sendoff.obj
		if !obj.hdr.IsLast() {
			if glog.V(4) {
				glog.Infof("%s sent header %s/%s(%d)", s.lid, obj.hdr.Bucket, obj.hdr.Objname, obj.hdr.Dsize)
			}
		} else {
			if glog.V(4) {
				glog.Infof("%s sent last", s.lid)
			}
			err = io.EOF
			s.lastCh <- struct{}{}
		}
	}
	return
}

func (s *Stream) sendData(b []byte) (n int, err error) {
	n, err = s.sendoff.obj.reader.Read(b)
	s.sendoff.off += int64(n)
	if glog.V(4) {
		glog.Infof("%s offset=%d (%d/%d)", s.lid, s.sendoff.off, s.stats.numcur, s.stats.numtot)
	}
	if err != nil {
		if err == io.EOF {
			s.eoObj()
			err = nil
		}
	} else if s.sendoff.off >= s.sendoff.obj.hdr.Dsize {
		s.eoObj()
	}
	return
}

func (s *Stream) eoObj() {
	s.stats.numtot++
	s.stats.numcur++
	s.stats.sizetot += s.sendoff.off
	s.stats.sizecur += s.sendoff.off
	if s.sendoff.off != s.sendoff.obj.hdr.Dsize {
		glog.Errorf("%s offset %d != %d size", s.lid, s.sendoff.off, s.sendoff.obj.hdr.Dsize)
	} else {
		if glog.V(4) {
			glog.Infof("%s sent size=%d (%d/%d)", s.lid, s.sendoff.obj.hdr.Dsize, s.stats.numcur, s.stats.numtot)
		}
	}

	_ = s.sendoff.obj.reader.Close()
	s.time.idle.Reset(s.time.idleOut)
	s.sendoff = sendoff{}
}

//
// stream helpers
//
func (s *Stream) insHeader(hdr Header) (l int) {
	l = sizeofI64
	l = insString(l, s.maxheader, hdr.Bucket)
	l = insString(l, s.maxheader, hdr.Objname)
	l = insByte(l, s.maxheader, hdr.Opaque)
	l = insInt64(l, s.maxheader, hdr.Dsize)
	insInt64(0, s.maxheader, int64(l-sizeofI64))
	return
}

func insString(off int, to []byte, str string) int {
	var l = len(str)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += sizeofI64
	n := copy(to[off:], str)
	if debug {
		common.Assert(n == l)
	}
	return off + l
}

func insByte(off int, to []byte, b []byte) int {
	var l = len(b)
	binary.BigEndian.PutUint64(to[off:], uint64(l))
	off += sizeofI64
	n := copy(to[off:], b)
	if debug {
		common.Assert(n == l)
	}
	return off + l
}

func insInt64(off int, to []byte, i int64) int {
	if debug {
		common.Assert(len(to) >= off+sizeofI64)
	}
	binary.BigEndian.PutUint64(to[off:], uint64(i))
	return off + sizeofI64
}
