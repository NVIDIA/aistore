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
		TCPBufSize       int
		IdleConnsPerHost int
		UseHTTPS         bool
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
		idleConnsPerHost = args.IdleConnsPerHost
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
	if args.TCPBufSize > 0 {
		dialer.Control = args.setSockOpt
	}
	transport := &http.Transport{
		Proxy:                 defaultTransport.Proxy,
		DialContext:           dialer.DialContext,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		MaxIdleConnsPerHost:   idleConnsPerHost,
		MaxIdleConns:          0, // no limit
	}
	if args.UseHTTPS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
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
		err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, args.TCPBufSize)
		AssertNoErr(err)
		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, args.TCPBufSize)
		AssertNoErr(err)
	}
	return
}

func (args *TransportArgs) setSockOpt(_, _ string, c syscall.RawConn) (err error) {
	return c.Control(args.ConnControl(c))
}
