//go:build debug

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

var cbUSR1 func()

func SetUSR1(cb func()) { cbUSR1 = cb }

func (hk *hk) setSignal() {
	signal.Notify(hk.sigCh,
		// ignore, log
		syscall.SIGHUP, // kill -SIGHUP
		// terminate
		syscall.SIGINT,  // kill -SIGINT (Ctrl-C)
		syscall.SIGTERM, // kill -SIGTERM
		syscall.SIGQUIT, // kill -SIGQUIT
		// test
		syscall.SIGUSR1,
	)
}

func (hk *hk) handleSignal(s syscall.Signal) (err error) {
	switch s {
	case syscall.SIGHUP:
		var (
			sb  cos.SB
			mem sys.MemStat
			ngr = runtime.NumGoroutine()
		)
		erm := mem.Get()
		mem.Str(&sb)
		nfd, erf := numOpenFiles()
		nlog.Infoln("ngr [", ngr, sys.NumCPU(), "] mem [", sb.String(), erm, "]", "num-fd [", nfd, erf, "]")
	case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
		signal.Stop(hk.sigCh)
		err = cos.NewSignalError(s)
		hk.Stop(err)
	case syscall.SIGUSR1:
		cbUSR1()
	default:
		cos.ExitLog("unexpected signal:", s)
	}

	return err
}
