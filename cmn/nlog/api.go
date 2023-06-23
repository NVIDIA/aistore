// Package nlog - aistore logger, provides buffering, timestamping, writing, and
// flushing/syncing/rotating
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import "flag"

var (
	MaxSize int64 = 4 * 1024 * 1024
)

func InitFlags(flset *flag.FlagSet) {
	flset.BoolVar(&toStderr, "logtostderr", false, "log to standard error instead of files")
	flset.BoolVar(&alsoToStderr, "alsologtostderr", false, "log to standard error as well as files")
}

func InfoDepth(depth int, args ...any)    { log(sevInfo, depth, "", args...) }
func Infoln(args ...any)                  { log(sevInfo, 0, "", args...) }
func Infof(format string, args ...any)    { log(sevInfo, 0, format, args...) }
func Warningln(args ...any)               { log(sevWarn, 0, "", args...) }
func Warningf(format string, args ...any) { log(sevWarn, 0, format, args...) }
func ErrorDepth(depth int, args ...any)   { log(sevErr, depth, "", args...) }
func Errorln(args ...any)                 { log(sevErr, 0, "", args...) }
func Errorf(format string, args ...any)   { log(sevErr, 0, format, args...) }

func SetLogDirRole(dir, role string) { logDir, aisrole = dir, role }
func SetTitle(s string)              { title = s }

func InfoLogName() string { return sname() + ".INFO" }
func ErrLogName() string  { return sname() + ".ERROR" }

func Flush() {
	nlogs[sevErr].flush(false)
	nlogs[sevInfo].flush(false)
}

func FlushExit() {
	nlogs[sevErr].flushExit()
	nlogs[sevInfo].flushExit()
}
