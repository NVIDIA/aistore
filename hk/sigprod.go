//go:build !debug

// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"os/signal"
	"runtime"
	"syscall"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
)

func (hk *hk) setSignal() {
	signal.Notify(hk.sigCh,
		// ignore, log
		syscall.SIGHUP, // kill -SIGHUP
		// terminate
		syscall.SIGINT,  // kill -SIGINT (Ctrl-C)
		syscall.SIGTERM, // kill -SIGTERM
		syscall.SIGQUIT, // kill -SIGQUIT
	)
}

func (hk *hk) handleSignal(s syscall.Signal) error {
	if s == syscall.SIGHUP {
		// no-op: show up in the log with some useful info
		var (
			sb  cos.SB
			mem sys.MemStat
			ngr = runtime.NumGoroutine()
		)
		erm := mem.Get()
		mem.Str(&sb)
		nfd, erf := numOpenFiles()
		nlog.Infoln("ngr [", ngr, sys.NumCPU(), "] mem [", sb.String(), erm, "]", "num-fd [", nfd, erf, "]")
		return nil
	}

	signal.Stop(hk.sigCh)
	err := cos.NewSignalError(s)
	hk.Stop(err)
	return err
}
