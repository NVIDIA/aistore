// Package glog
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package glog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// nlog replaces glog's original buffering, writing, and flushing/syncing

const (
	nlogBufSize          = 64 * 1024
	nlogBufFlushBoundary = 512
)

type (
	nlog struct {
		pw, pf     *bytes.Buffer
		file       *os.File
		buf1, buf2 *bytes.Buffer
		line       linebuf
		rsize      int64
		sev        severity
		err        error
		mw, mf     sync.Mutex
	}
	errLogAborted struct {
		err error
	}
)

func newErrLogAborted(err error) error {
	e := &errLogAborted{err}
	os.Stderr.WriteString(e.Error())
	return e
}

func (e *errLogAborted) Error() string { return fmt.Sprintf("logging stopped: %v", e.err) }

//
// nlog
//

func newNlog(sev severity) *nlog {
	nlog := &nlog{
		sev:  sev,
		buf1: bytes.NewBuffer(make([]byte, nlogBufSize)),
		buf2: bytes.NewBuffer(make([]byte, nlogBufSize)),
	}
	nlog.buf1.Reset()
	nlog.buf2.Reset()
	nlog.line.Grow(1024) // for life
	nlog.pw = nlog.buf1
	return nlog
}

func (nlog *nlog) printf(sev severity, depth int, format string, args ...any) {
	nlog.mw.Lock()
	nlog.line.Reset()
	sprintf(sev, depth+1, format, &nlog.line, args...)
	if nlog.err != nil {
		nlog.mw.Unlock()
		os.Stderr.Write(nlog.line.Bytes())
		return
	}
	nlog._write(nlog.line.Bytes())
	nlog.mw.Unlock()
}

func (nlog *nlog) write(p []byte) {
	nlog.mw.Lock()
	if nlog.err != nil {
		nlog.mw.Unlock()
		os.Stderr.Write(p)
		return
	}
	nlog._write(p)
	nlog.mw.Unlock()
}

func (nlog *nlog) _write(p []byte) {
	rem := nlog.pw.Cap() - nlog.pw.Len()
	assert(rem >= nlogBufFlushBoundary, "unexpected remaining length", nlog.pw.Len(), nlogBufSize, rem)
	if len(p) > rem {
		p = p[:rem] // truncate in an unlikely event
	}
	n, err := nlog.pw.Write(p)
	nlog.rsize += int64(n)
	nlog.err = err

	flush := nlog.pw.Len() >= nlogBufSize-nlogBufFlushBoundary && err == nil
	if !flush {
		return
	}

	// under mw locked - other writers will have to wait at (W)
	rsize := nlog.rsize
	nlog.mf.Lock()
	nlog.swap()
	pf := nlog.pf
	nlog.mf.Unlock()

	go nlog._flush(pf, rsize) // TODO: linger for awhile and reuse
}

func (nlog *nlog) swap() {
	if nlog.pf != nil {
		nlog.mf.Unlock()
		nlog.pollPrevFlush() // (W)
	}
	nlog.pf = nlog.pw
	if nlog.pw == nlog.buf1 {
		nlog.pw = nlog.buf2
	} else {
		nlog.pw = nlog.buf1
	}
	assert(nlog.pw.Len() == 0, "expecting write buf to have zero length", nlog.pw.Len())
}

func (nlog *nlog) _flush(pf *bytes.Buffer, rsize int64) {
	n, err := nlog.file.Write(pf.Bytes())
	if err != nil || n != pf.Len() {
		if err == nil {
			err = fmt.Errorf("write's too short (%d < %d)", n, pf.Len())
		}
		os.Stderr.WriteString(err.Error())
	}
	if rsize >= MaxSize {
		nlog.file.Close()
		nlog.mw.Lock()
		nlog.rotate(time.Now(), rsize)
		nlog.mw.Unlock()
	}
	nlog.mf.Lock()
	assert(pf == nlog.pf, fmt.Sprintf("buffer-to-flush has changed: %+v (%p) != %+v (%p)", pf, pf, nlog.pf, nlog.pf))
	if pf == nlog.pf {
		nlog.pf.Reset()
		nlog.pf = nil // done
	}
	nlog.mf.Unlock()
}

// executes under mw lock, returns with mf lock taken
func (nlog *nlog) pollPrevFlush() {
	const sleep = 2 * time.Second
	var total time.Duration
	for {
		time.Sleep(sleep)
		nlog.mf.Lock()
		if nlog.pf == nil {
			return
		}
		nlog.mf.Unlock()
		total += sleep
		if total >= time.Minute {
			err := errors.New("nlog flush takes exceedingly long time: " + total.String())
			if total <= time.Minute+sleep {
				os.Stderr.WriteString(err.Error())
			} else if total > 5*time.Minute && nlog.err == nil {
				nlog.err = newErrLogAborted(err)
			}
		}
	}
}

func (nlog *nlog) flush(waitPrev bool) {
	nlog.mw.Lock()
	if nlog.err != nil || nlog.pw.Len() == 0 {
		nlog.mw.Unlock()
		return
	}
	rsize := nlog.rsize
	nlog.mf.Lock()
	// unless explicitly requested don't insist if it's currently running
	if !waitPrev && nlog.pf != nil {
		nlog.mf.Unlock()
		nlog.mw.Unlock()
		return
	}
	nlog.swap()
	pf := nlog.pf
	nlog.mf.Unlock()
	nlog.mw.Unlock()

	nlog._flush(pf, rsize)
}

func (nlog *nlog) flushExit() {
	nlog.flush(true)
	nlog.file.Sync()
}

func (nlog *nlog) rotate(now time.Time, rsize int64) (err error) {
	var (
		n    int
		x    = fmt.Sprintf("host %s, %s for %s/%s\n", host, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		snow = now.Format("2006/01/02 15:04:05")
	)
	if nlog.file, _, err = fcreate(severityName[nlog.sev], now); err != nil {
		nlog.err = newErrLogAborted(err)
		return
	}
	nlog.rsize -= rsize
	assert(nlog.rsize >= 0, "negative rsize", nlog.rsize)

	// direct write under w-lock
	if FileHeaderCB == nil {
		n, err = fmt.Fprintf(nlog.file, "Started up at %s, %s", snow, x)
	} else {
		n1, _ := fmt.Fprintf(nlog.file, "Created at %s, %s", snow, x)
		n, err = fmt.Fprint(nlog.file, FileHeaderCB())
		n += n1
	}
	nlog.rsize += int64(n)
	if err != nil {
		assert(false, err)
		nlog.err = newErrLogAborted(err)
	}
	return
}

//
// linebuf mem pool - used for errors and warnings only
//

func alloc() (buf *linebuf) {
	if v := pool.Get(); v != nil {
		buf = v.(*linebuf)
		buf.Reset()
	} else {
		buf = &linebuf{}
	}
	return
}

func free(buf *linebuf) { pool.Put(buf) }
