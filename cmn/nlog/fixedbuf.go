// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"io"
	"os"
	"time"
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

// "15:04:05.000000"
func (fb *fixed) writeStamp() {
	now := time.Now()
	hour, minute, second := now.Clock()

	fb.ab(hour)
	fb.buf[fb.woff] = ':'
	fb.woff++

	fb.ab(minute)
	fb.buf[fb.woff] = ':'
	fb.woff++

	fb.ab(second)
	fb.buf[fb.woff] = '.'
	fb.woff++

	fb.abcdef(now.Nanosecond() / 1000)
}

const digits = "0123456789"

func (fb *fixed) ab(d int) {
	fb.buf[fb.woff] = digits[d/10]
	fb.woff++

	fb.buf[fb.woff] = digits[d%10]
	fb.woff++
}

func (fb *fixed) abcdef(millis int) {
	j := 5
	for ; j >= 0 && millis > 0; j-- {
		fb.buf[fb.woff+j] = digits[millis%10]
		millis /= 10
	}

	const pad = '0'
	for ; j >= 0; j-- {
		fb.buf[fb.woff+j] = pad
	}
	fb.woff += 6
}

func (fb *fixed) flush(file *os.File) (n int, err error) {
	n, err = file.Write(fb.buf[:fb.woff])
	if err != nil {
		if Stopping() {
			_whileStopping(fb.buf[:fb.woff])
			return 0, nil
		}
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
