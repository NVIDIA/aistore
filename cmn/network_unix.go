// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	if args.Egress != EgressAny {
		return args.guardEgress
	}
	return args.sockControl()
}

// TransportArgs.Egress checks the resolved destination
func (args *TransportArgs) guardEgress(network, address string, c syscall.RawConn) error {
	if err := checkEgress(network, address, args.Egress == EgressAllowPrivate); err != nil {
		return err
	}
	if ctrl := args.sockControl(); ctrl != nil {
		return ctrl(network, address, c)
	}
	return nil
}

func (args *TransportArgs) sockControl() cntlFunc {
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

func (args *TransportArgs) setSockSndRcv(_, _ string, c syscall.RawConn) error {
	return c.Control(args._sndrcv)
}

// buffering is limited by /proc/sys/net/core/rmem_max and /proc/sys/net/core/wmem_max, respectively
func (args *TransportArgs) _sndrcv(fd uintptr) {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, args.SndRcvBufSize)
	_croak(err)

	err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, args.SndRcvBufSize)
	_croak(err)
}

func (args *TransportArgs) setSockToS(_, _ string, c syscall.RawConn) error {
	return c.Control(args._tos)
}

func (args *TransportArgs) _tos(fd uintptr) {
	var err error
	if args.PreferIPv6 {
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_TCLASS, lowDelayToS)
		if err == nil {
			return
		}
		// dual-stack dialer may have created a v4 socket — fall through
	}
	err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, lowDelayToS)
	_croak(err)
}

func (args *TransportArgs) setSockSndRcvToS(_, _ string, c syscall.RawConn) error {
	return c.Control(args._sndrcvtos)
}

func (args *TransportArgs) _sndrcvtos(fd uintptr) {
	args._sndrcv(fd)
	args._tos(fd)
}

func _croak(err error) {
	if err != nil {
		nlog.ErrorDepth(1, err)
	}
}
