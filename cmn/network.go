// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

const (
	NetworkPublic       = "public"
	NetworkIntraControl = "intra_control"
	NetworkIntraData    = "intra_data"
)

var (
	KnownNetworks = []string{NetworkPublic, NetworkIntraControl, NetworkIntraData}
)

type (
	TransportArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
		SndRcvBufSize    int
		IdleConnsPerHost int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPS         bool
		UseHTTPProxyEnv  bool
	}
)

func NetworkIsKnown(net string) bool {
	return net == NetworkPublic || net == NetworkIntraControl || net == NetworkIntraData
}

func ParsePort(p string) (int, error) {
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}

	if port <= 0 || port >= (1<<16) {
		return 0, fmt.Errorf("port number (%d) should be between 1 and 65535", port)
	}

	return port, nil
}

func NewTransport(args TransportArgs) *http.Transport {
	var (
		dialTimeout      = args.DialTimeout
		defaultTransport = http.DefaultTransport.(*http.Transport)
	)

	if dialTimeout == 0 {
		dialTimeout = 30 * time.Second
	}
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}
	// setsockopt when non-zero, otherwise use TCP defaults
	if args.SndRcvBufSize > 0 {
		dialer.Control = args.setSockOpt
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		MaxIdleConnsPerHost:   args.IdleConnsPerHost,
		WriteBufferSize:       args.WriteBufferSize,
		ReadBufferSize:        args.ReadBufferSize,
		MaxIdleConns:          0, // no limit
	}
	if args.UseHTTPS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if args.UseHTTPProxyEnv {
		transport.Proxy = defaultTransport.Proxy
	}
	return transport
}

func NewClient(args TransportArgs) *http.Client {
	transport := NewTransport(args)
	client := &http.Client{
		Transport: transport,
		Timeout:   args.Timeout,
	}
	return client
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

func (args *TransportArgs) setSockOpt(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args.ConnControl(c))
}
