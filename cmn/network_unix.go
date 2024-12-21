// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"syscall"

	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// ref: https://linuxreviews.org/Type_of_Service_(ToS)_and_DSCP_Values
const (
	lowDelayToS = 0x10
)

type (
	cntlFunc func(network, address string, c syscall.RawConn) error
)

func (args *TransportArgs) ServerControl(c syscall.RawConn) {
	switch {
	case args.SndRcvBufSize > 0 && args.LowLatencyToS:
		c.Control(args._sndrcvtos)
	case args.SndRcvBufSize > 0:
		c.Control(args._sndrcv)
	case args.LowLatencyToS && !Rom.Features().IsSet(feat.DontSetControlPlaneToS):
		c.Control(args._tos)
	}
}

func (args *TransportArgs) clientControl() cntlFunc {
	switch {
	case args.SndRcvBufSize > 0 && args.LowLatencyToS:
		return args.setSockSndRcvToS
	case args.SndRcvBufSize > 0:
		return args.setSockSndRcv
	case args.LowLatencyToS && !Rom.Features().IsSet(feat.DontSetControlPlaneToS):
		return args.setSockToS
	}
	return nil
}

//
//--------------------------- low level internals
//

func (args *TransportArgs) setSockSndRcv(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args._sndrcv)
}

// buffering is limited by /proc/sys/net/core/rmem_max and /proc/sys/net/core/wmem_max, respectively
func (args *TransportArgs) _sndrcv(fd uintptr) {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, args.SndRcvBufSize)
	_croak(err)

	err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, args.SndRcvBufSize)
	_croak(err)
}

func (args *TransportArgs) setSockToS(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args._tos)
}

func (*TransportArgs) _tos(fd uintptr) {
	err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, lowDelayToS)
	_croak(err)
}

func (args *TransportArgs) setSockSndRcvToS(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args._sndrcvtos)
}

func (args *TransportArgs) _sndrcvtos(fd uintptr) {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, args.SndRcvBufSize)
	_croak(err)

	err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, args.SndRcvBufSize)
	_croak(err)

	err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, lowDelayToS)
	_croak(err)
}

func _croak(err error) {
	if err != nil {
		nlog.ErrorDepth(1, err)
	}
}
