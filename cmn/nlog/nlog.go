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
	fixedSize   = 64 * 1024
	extraSize   = 32 * 1024 // via mem pool
	maxLineSize = 2 * 1024
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
		line           fixed
		toFlush        []*fixed
		last           atomic.Int64
		written        atomic.Int64
		sev            severity
		oob            atomic.Bool
		erred          atomic.Bool
		mw             sync.Mutex
	}
)

// main function
func log(sev severity, depth int, format string, args ...any) {
	onceInitFiles.Do(initFiles)

	switch {
	case !flag.Parsed():
		os.Stderr.WriteString("Error: logging before flag.Parse: ")
		fallthrough
	case LogToStderr:
		fb := alloc()
		sprintf(sev, depth, format, fb, args...)
		_, err := fb.flush(os.Stderr)
		assert(err == nil)
		free(fb)
	case sev >= sevWarn:
		fb := alloc()
		sprintf(sev, depth, format, fb, args...)
		if sev >= sevErr {
			_, err := fb.flush(os.Stderr)
			assert(err == nil)
		}
		if sev >= sevWarn {
			nlog := nlogs[sevErr]
			nlog.mw.Lock()
			nlog.write(fb)
			nlog.mw.Unlock()
		}
		nlog := nlogs[sevInfo]
		nlog.mw.Lock()
		nlog.write(fb)
		nlog.mw.Unlock()
		free(fb)
	default:
		// fast path
		nlogs[sevInfo].printf(sev, depth, format, args...)
	}
}

//
// nlog
//

func newNlog(sev severity) *nlog {
	nlog := &nlog{
		sev:     sev,
		buf1:    &fixed{buf: make([]byte, fixedSize)},
		buf2:    &fixed{buf: make([]byte, fixedSize)},
		line:    fixed{buf: make([]byte, maxLineSize)},
		toFlush: make([]*fixed, 0, 4),
	}
	nlog.pw = nlog.buf1
	return nlog
}

func (nlog *nlog) since(now int64) time.Duration { return time.Duration(now - nlog.last.Load()) }

func (nlog *nlog) printf(sev severity, depth int, format string, args ...any) {
	nlog.mw.Lock()
	nlog.line.reset()
	sprintf(sev, depth+1, format, &nlog.line, args...)
	nlog.write(&nlog.line)
	nlog.mw.Unlock()
}

// under mw-lock
func (nlog *nlog) write(line *fixed) {
	buf := line.buf[:line.woff]
	nlog.pw.Write(buf)

	if nlog.pw.avail() > maxLineSize {
		return
	}

	nlog.toFlush = append(nlog.toFlush, nlog.pw)
	nlog.oob.Store(true)
	nlog.get()
}

func (nlog *nlog) get() {
	prev := nlog.pw
	assert(prev == nlog.toFlush[len(nlog.toFlush)-1])
	switch prev {
	case nlog.buf1:
		if nlog.buf2 != nil {
			nlog.pw = nlog.buf2
		} else {
			nlog.pw = alloc()
		}
		nlog.buf1 = nil
	case nlog.buf2:
		if nlog.buf1 != nil {
			nlog.pw = nlog.buf1
		} else {
			nlog.pw = alloc()
		}
		nlog.buf2 = nil
	default: // prev was alloc-ed
		switch {
		case nlog.buf1 != nil:
			nlog.pw = nlog.buf1
		case nlog.buf2 != nil:
			nlog.pw = nlog.buf2
		default:
			nlog.pw = alloc()
		}
	}
}

func (nlog *nlog) put(pw *fixed /* to reuse */) {
	nlog.mw.Lock()
	switch {
	case nlog.buf1 == nil:
		nlog.buf1 = pw
	case nlog.buf2 == nil:
		nlog.buf2 = pw
	default:
		assert(nlog.buf1 == pw || nlog.buf2 == pw) // via Flush(true)
	}
	nlog.mw.Unlock()
}

func (nlog *nlog) flush() {
	for {
		nlog.mw.Lock()
		if len(nlog.toFlush) == 0 {
			nlog.oob.Store(false)
			nlog.mw.Unlock()
			break
		}
		pw := nlog.toFlush[0]
		copy(nlog.toFlush, nlog.toFlush[1:])
		nlog.toFlush = nlog.toFlush[:len(nlog.toFlush)-1]
		nlog.mw.Unlock()

		nlog.do(pw)
	}
}

func (nlog *nlog) do(pw *fixed) {
	// write
	if nlog.erred.Load() {
		if Stopping() {
			_whileStopping(pw.buf[:pw.woff])
		} else {
			os.Stderr.Write(pw.buf[:pw.woff])
		}
	} else {
		n, err := pw.flush(nlog.file)
		if err != nil {
			nlog.erred.Store(true)
		}
		nlog.written.Add(int64(n))
		nlog.last.Store(mono.NanoTime())
	}

	// recycle buf
	pw.reset()
	if pw.size() == extraSize {
		free(pw)
	} else {
		assert(pw.size() == fixedSize)
		nlog.put(pw)
	}

	// rotate
	if nlog.written.Load() >= MaxSize {
		err := nlog.file.Close()
		assert(err == nil)
		nlog.rotate(time.Now())
	}
}

func (nlog *nlog) rotate(now time.Time) (err error) {
	var (
		line1 string
		s     = fmt.Sprintf("host %s, %s for %s/%s\n", host, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		snow  = now.Format("2006/01/02 15:04:05")
	)
	if nlog.file, _, err = fcreate(sevText[nlog.sev], now); err != nil {
		nlog.erred.Store(true)
		return
	}

	nlog.written.Store(0)
	nlog.erred.Store(false)
	if title == "" {
		line1 = "Started up at " + snow + ", " + s
		_, err = nlog.file.WriteString(line1)
	} else {
		line1 = "Rotated at " + snow + ", " + s
		nlog.file.WriteString(line1)
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

	fb.writeStamp()

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
		fmt.Fprintln(fb, args...)
	} else {
		fmt.Fprintf(fb, format, args...)
		fb.eol()
	}
}

const wstag = "[while stopping:] "

func _whileStopping(p []byte) {
	os.Stderr.WriteString(wstag)
	os.Stderr.Write(p)
	os.Stderr.WriteString("\n")
}

// mem pool of additional buffers
// usage:
// - none of the "fixed" ones available
// - alsoToStderr

func alloc() (fb *fixed) {
	if v := pool.Get(); v != nil {
		fb = v.(*fixed)
		fb.reset()
	} else {
		fb = &fixed{buf: make([]byte, extraSize)}
	}
	return
}

func free(fb *fixed) {
	assert(fb.size() == extraSize)
	pool.Put(fb)
}
