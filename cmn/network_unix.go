// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "syscall"

func (args *TransportArgs) setSockOpt(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args.ConnControl(c))
}

func (args *TransportArgs) ConnControl(c syscall.RawConn) (cntl func(fd uintptr)) {
	cntl = func(fd uintptr) {
		// NOTE: is limited by /proc/sys/net/core/rmem_max
		err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, args.SndRcvBufSize)
		AssertNoErr(err)
		// NOTE: is limited by /proc/sys/net/core/wmem_max
		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, args.SndRcvBufSize)
		AssertNoErr(err)
	}
	return
}
