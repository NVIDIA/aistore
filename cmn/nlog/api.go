// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

const (
	ActNone = iota
	ActExit
	ActRotate
)

var LogToStderr bool
var MaxSize int64 = 4 * 1024 * 1024 // usually, config.log.max_size

func InfoDepth(depth int, args ...any)    { log(sevInfo, depth, "", args...) }
func Infoln(args ...any)                  { log(sevInfo, 0, "", args...) }
func Infof(format string, args ...any)    { log(sevInfo, 0, format, args...) }
func Warningln(args ...any)               { log(sevWarn, 0, "", args...) }
func Warningf(format string, args ...any) { log(sevWarn, 0, format, args...) }
func ErrorDepth(depth int, args ...any)   { log(sevErr, depth, "", args...) }
func Errorln(args ...any)                 { log(sevErr, 0, "", args...) }
func Errorf(format string, args ...any)   { log(sevErr, 0, format, args...) }

func SetPre(dir, role string) {
	logDir, aisrole = dir, role
}

func SetPost(logToStderr bool, maxSize int64) {
	LogToStderr = logToStderr
	MaxSize = maxSize
	if MaxSize > 1024*1024*1024 {
		Warningf("log.max_size %d exceeds 1GB, setting log.max_size=4MB", MaxSize)
		MaxSize = 4 * 1024 * 1024
	}
}

func SetTitle(s string) { title = s }

func InfoLogName() string { return sname() + ".INFO" }
func ErrLogName() string  { return sname() + ".ERROR" }

func Flush(action int) {
	now := mono.NanoTime()
	for _, sev := range []severity{sevInfo, sevErr} {
		var (
			nlog = nlogs[sev]
			oob  bool
		)

		nlog.mw.Lock()
		if nlog.file == nil || (nlog.pw.length() == 0 && action != ActRotate) {
			nlog.mw.Unlock()
			continue
		}
		if action > ActNone || nlog.pw.avail() < maxLineSize || nlog.since(now) > 10*time.Second {
			nlog.toFlush = append(nlog.toFlush, nlog.pw)
			nlog.get()
		}
		oob = len(nlog.toFlush) > 0
		nlog.mw.Unlock()

		if oob {
			nlog.flush()
		}
		if action == ActRotate {
			nlog.rotate(time.Now())
		}
		if action == ActExit {
			nlog.file.Sync()
			nlog.file.Close()
		}
	}
}

func Since() time.Duration {
	now := mono.NanoTime()
	a, b := nlogs[sevInfo].since(now), nlogs[sevErr].since(now)
	if a > b {
		return a
	}
	return b
}

func OOB() bool {
	return nlogs[sevInfo].oob.Load() || nlogs[sevErr].oob.Load()
}

func Stopping() bool { return stopping.Load() }
func SetStopping()   { stopping.Store(true) }
