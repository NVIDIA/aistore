// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	NetworkPublic       = "PUBLIC"
	NetworkIntraControl = "INTRA-CONTROL"
	NetworkIntraData    = "INTRA-DATA"
)

// NOTE: as of Go 1.16, http.DefaultTransport has the following defaults:
//
//       MaxIdleConns:          100,
//       MaxIdleConnsPerHost :  2 (via DefaultMaxIdleConnsPerHost)
//       IdleConnTimeout:       90 * time.Second,
//       WriteBufferSize:       4KB
//       ReadBufferSize:        4KB
//
// Following are the constants we use by default:

const (
	DefaultMaxIdleConns        = 64
	DefaultMaxIdleConnsPerHost = 16
	DefaultIdleConnTimeout     = 8 * time.Second
	DefaultWriteBufferSize     = 64 * cos.KiB
	DefaultReadBufferSize      = 64 * cos.KiB
	DefaultSendRecvBufferSize  = 128 * cos.KiB
)

var KnownNetworks = []string{NetworkPublic, NetworkIntraControl, NetworkIntraData}

type (
	// Options to create a transport for HTTP client
	TransportArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
		IdleConnTimeout  time.Duration
		IdleConnsPerHost int
		MaxIdleConns     int
		SndRcvBufSize    int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPS         bool
		UseHTTPProxyEnv  bool
		// For HTTPS mode only: if true, the client does not verify server's
		// certificate. It is useful for clusters with self-signed certificates.
		SkipVerify bool
	}
)

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
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		IdleConnTimeout:       args.IdleConnTimeout,
		MaxIdleConnsPerHost:   args.IdleConnsPerHost,
		MaxIdleConns:          args.MaxIdleConns,
		WriteBufferSize:       args.WriteBufferSize,
		ReadBufferSize:        args.ReadBufferSize,
	}

	// apply global defaults
	if transport.MaxIdleConnsPerHost == 0 {
		transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	if transport.MaxIdleConns == 0 {
		transport.MaxIdleConns = DefaultMaxIdleConns
	}
	if transport.IdleConnTimeout == 0 {
		transport.IdleConnTimeout = DefaultIdleConnTimeout
	}
	if transport.WriteBufferSize == 0 {
		transport.WriteBufferSize = DefaultWriteBufferSize
	}
	if transport.ReadBufferSize == 0 {
		transport.ReadBufferSize = DefaultReadBufferSize
	}

	if args.UseHTTPS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: args.SkipVerify}
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

// misc helpers

func NetworkIsKnown(net string) bool {
	return net == NetworkPublic || net == NetworkIntraControl || net == NetworkIntraData
}

func ParsePort(p string) (int, error) {
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}

	return ValidatePort(port)
}

func ValidatePort(port int) (int, error) {
	if port <= 0 || port >= (1<<16) {
		return 0, fmt.Errorf("port number (%d) should be between 1 and 65535", port)
	}
	return port, nil
}
