// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

const (
	nlogBufSize  = 64 * 1024
	nlogLineSize = 4 * 1024
	nlogChSize   = 32
)

type severity int

const (
	sevInfo severity = iota
	sevWarn
	sevErr
)

type (
	nlog struct {
		file           *os.File
		pw, buf1, buf2 *fixed
		ch             chan *fixed
		line           fixed
		last           atomic.Int64
		sev            severity
		mw             sync.Mutex
	}
)

//
// nlog
//

func newNlog(sev severity) *nlog {
	nlog := &nlog{
		sev:  sev,
		buf1: &fixed{buf: make([]byte, nlogBufSize)},
		buf2: &fixed{buf: make([]byte, nlogBufSize)},
		line: fixed{buf: make([]byte, nlogLineSize)},
		ch:   make(chan *fixed, nlogChSize),
	}
	nlog.pw = nlog.buf1

	go nlog.flusher()
	return nlog
}

// main function
func log(sev severity, depth int, format string, args ...any) {
	onceInitFiles.Do(initFiles)

	switch {
	case !flag.Parsed():
		os.Stderr.WriteString("Error: logging before flag.Parse: ")
		fallthrough
	case toStderr:
		fb := alloc()
		sprintf(sev, depth, format, fb, args...)
		os.Stderr.Write(fb.buf[:fb.woff])
		free(fb)
	case alsoToStderr || sev >= sevWarn:
		fb := alloc()
		sprintf(sev, depth, format, fb, args...)
		if alsoToStderr || sev >= sevErr {
			os.Stderr.Write(fb.buf[:fb.woff])
		}
		nlog := nlogs[sevErr]
		nlog.mw.Lock()
		nlog.write(fb)
		nlog.mw.Unlock()

		nlog = nlogs[sevInfo]
		nlog.mw.Lock()
		nlog.write(fb)
		nlog.mw.Unlock()
		free(fb)
	default:
		// fast path
		nlogs[sevInfo].printf(sev, depth, format, args...)
	}
}

func (nlog *nlog) printf(sev severity, depth int, format string, args ...any) {
	nlog.mw.Lock()
	nlog.line.reset()
	sprintf(sev, depth+1, format, &nlog.line, args...)
	nlog.write(&nlog.line)
	nlog.mw.Unlock()
}

func (nlog *nlog) flush(exit bool) {
	nlog.mw.Lock()
	if nlog.pw.woff > 0 {
		if exit {
			nlog.ch <- nlog.pw // always have two spare slots (see `cap` below)
			nlog.ch <- nil
		} else if (nlog.pw.avail() < nlogBufSize/2) || (mono.Since(nlog.last.Load()) > 10*time.Second) {
			if len(nlog.ch) < cap(nlog.ch)-1 { // ditto
				nlog.ch <- nlog.pw
				nlog.get()
			}
		}
	}
	nlog.mw.Unlock()
}

// under mw-lock
func (nlog *nlog) write(line *fixed) {
	buf := line.buf[:line.woff]
	nlog.pw.Write(buf)
	if nlog.pw.avail() > nlogLineSize {
		return
	}
	if len(nlog.ch) < cap(nlog.ch)-2 { // ditto
		nlog.ch <- nlog.pw
		nlog.get()
	} else {
		msg := fmt.Sprintf("Error: [nlog] drop %dB\n", nlog.pw.woff) // discard
		os.Stderr.WriteString(msg)
		nlog.pw.reset()
	}
}

func (nlog *nlog) get() {
	switch {
	case nlog.pw == nlog.buf1:
		if nlog.buf2 != nil {
			nlog.pw = nlog.buf2
		} else {
			nlog.pw = alloc()
		}
		nlog.buf1 = nil
	case nlog.pw == nlog.buf2:
		if nlog.buf1 != nil {
			nlog.pw = nlog.buf1
		} else {
			nlog.pw = alloc()
		}
		nlog.buf2 = nil
	default:
		if nlog.buf1 != nil {
			nlog.pw = nlog.buf1
		} else if nlog.buf2 != nil {
			nlog.pw = nlog.buf2
		} else {
			nlog.pw = alloc()
		}
	}
}

func (nlog *nlog) put(pw *fixed) {
	nlog.mw.Lock()
	if nlog.buf1 == nil {
		nlog.buf1 = pw
	} else {
		assert(nlog.buf2 == nil)
		nlog.buf2 = pw
	}
	nlog.mw.Unlock()
}

func (nlog *nlog) flusher() {
	var (
		n    int
		size int64
		err  error // fatal for this log
	)
	for {
		pw := <-nlog.ch
		// via FlushExit
		if pw == nil {
			nlog.file.Close()
			break
		}

		// do
		if err != nil { // log aborted
			os.Stderr.Write(pw.buf[:pw.woff])
		} else {
			n, err = nlog.file.Write(pw.buf[:pw.woff])
			if err != nil {
				os.Stderr.WriteString(err.Error())
				size = 0
			} else {
				size += int64(n)
			}
		}
		pw.reset()
		nlog.last.Store(mono.NanoTime())

		// recycle
		if cap(pw.buf) == nlogLineSize {
			free(pw)
		} else {
			assert(cap(pw.buf) == nlogBufSize)
			nlog.put(pw)
		}

		// rotate
		if size >= MaxSize {
			err = nlog.rotate(time.Now())
			size = 0
		}
	}
	// drain
	runtime.Gosched()
	for {
		select {
		case <-nlog.ch:
		default:
			return
		}
	}
}

func (nlog *nlog) rotate(now time.Time) (err error) {
	var (
		s    = fmt.Sprintf("host %s, %s for %s/%s\n", host, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		snow = now.Format("2006/01/02 15:04:05")
	)
	if nlog.file, _, err = fcreate(sevText[nlog.sev], now); err != nil {
		return
	}
	if title == "" {
		_, err = nlog.file.WriteString("Started up at " + snow + ", " + s)
	} else {
		nlog.file.WriteString("Rotated at " + snow + ", " + s)
		_, err = nlog.file.WriteString(title)
	}
	return
}

//
// utils
//

func logfname(tag string, t time.Time) (name, link string) {
	s := sname()
	name = fmt.Sprintf("%s.%s.%s.%02d%02d-%02d%02d%02d.%d",
		s,
		host,
		tag,
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		pid)
	return name, s + "." + tag
}

func formatHdr(s severity, depth int, fb *fixed) {
	const char = "IWE"
	_, fn, ln, ok := runtime.Caller(3 + depth)
	if !ok {
		return
	}
	idx := strings.LastIndexByte(fn, filepath.Separator)
	if idx > 0 {
		fn = fn[idx+1:]
	}
	if l := len(fn); l > 3 {
		fn = fn[:l-3]
	}
	fb.writeByte(char[s])
	fb.writeByte(' ')
	now := time.Now()
	fb.writeString(now.Format("15:04:05.000000"))

	fb.writeByte(' ')
	if _, redact := redactFnames[fn]; redact {
		return
	}
	fb.writeString(fn)
	fb.writeByte(':')
	fb.writeString(strconv.Itoa(ln))
	fb.writeByte(' ')
}

func sprintf(sev severity, depth int, format string, fb *fixed, args ...any) {
	formatHdr(sev, depth+1, fb)
	if format == "" {
		fmt.Fprint(fb, args...)
	} else {
		fmt.Fprintf(fb, format, args...)
	}
	fb.eol()
}

func fcreateAll(sev severity) error {
	now := time.Now()
	for s := sev; s >= sevInfo && nlogs[s] == nil; s-- {
		if s == sevWarn {
			continue
		}
		nlog := newNlog(s)
		if err := nlog.rotate(now); err != nil {
			return err
		}
		nlogs[s] = nlog
	}
	return nil
}

//
// buffer pool for errors and warnings
//

func alloc() (fb *fixed) {
	if v := pool.Get(); v != nil {
		fb = v.(*fixed)
		fb.reset()
	} else {
		fb = &fixed{buf: make([]byte, nlogLineSize)}
	}
	return
}

func free(fb *fixed) { pool.Put(fb) }
