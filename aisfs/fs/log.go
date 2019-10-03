// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"runtime/debug"
)

func (fs *aisfs) logf(fmt string, v ...interface{}) {
	fs.errLog.Printf(fmt, v...)
}

func (fs *aisfs) fatalf(fmt string, v ...interface{}) {
	errFmt := "FATAL: " + fmt +
		"\n*** CONNECTION LOST, BUT THE FILE SYSTEM REMAINS MOUNTED ON %s ***\n" +
		"CALL STACK ---> %s\n"
	v = append(v, fs.mountPath, debug.Stack())
	fs.errLog.Fatalf(errFmt, v...)
}
