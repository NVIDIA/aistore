// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"io"
	"os"
)

type fixed struct {
	buf  []byte
	woff int
}

// interface guard
var _ io.Writer = (*fixed)(nil)

func (fb *fixed) Write(p []byte) (int, error) {
	n := copy(fb.buf[fb.woff:], p)
	fb.woff += n
	return len(p), nil // silent discard
}

// private

func (fb *fixed) writeString(p string) {
	n := copy(fb.buf[fb.woff:], p)
	fb.woff += n
}

func (fb *fixed) writeByte(c byte) {
	if fb.avail() > 0 {
		fb.buf[fb.woff] = c
		fb.woff++
	}
}

func (fb *fixed) flush(file *os.File) (n int, err error) {
	n, err = file.Write(fb.buf[:fb.woff])
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
	}
	return
}

func (fb *fixed) reset()      { fb.woff = 0 }
func (fb *fixed) length() int { return fb.woff }
func (fb *fixed) size() int   { return cap(fb.buf) }
func (fb *fixed) avail() int  { return cap(fb.buf) - fb.woff }

func (fb *fixed) eol() {
	if fb.woff == 0 || (fb.buf[fb.woff-1] != '\n' && fb.avail() > 0) {
		fb.buf[fb.woff] = '\n'
		fb.woff++
	}
}
