// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	// assorted http(s) client options
	TransportArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
		IdleConnTimeout  time.Duration
		IdleConnsPerHost int
		MaxIdleConns     int
		SndRcvBufSize    int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPProxyEnv  bool
		// when true, requires TLSArgs (below)
		UseHTTPS bool
	}
	TLSArgs struct {
		SkipVerify bool
	}
)

// {TransportArgs + defaults} => http.Transport for a variety of ais clients
// NOTE: TLS below, and separately
func NewTransport(cargs TransportArgs) *http.Transport {
	var (
		dialTimeout      = cargs.DialTimeout
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
	if cargs.SndRcvBufSize > 0 {
		dialer.Control = cargs.setSockOpt
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		IdleConnTimeout:       cargs.IdleConnTimeout,
		MaxIdleConnsPerHost:   cargs.IdleConnsPerHost,
		MaxIdleConns:          cargs.MaxIdleConns,
		WriteBufferSize:       cargs.WriteBufferSize,
		ReadBufferSize:        cargs.ReadBufferSize,
		DisableCompression:    true, // NOTE: hardcoded - never used
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
	// not used anymore
	if cargs.UseHTTPProxyEnv {
		transport.Proxy = defaultTransport.Proxy
	}
	return transport
}

func NewTLS(sargs TLSArgs) (*tls.Config, error) {
	return &tls.Config{InsecureSkipVerify: sargs.SkipVerify}, nil
}

func NewClient(cargs TransportArgs) *http.Client {
	cos.AssertMsg(!cargs.UseHTTPS, "NewClientTLS must be called instead")
	return &http.Client{Transport: NewTransport(cargs), Timeout: cargs.Timeout}
}

func NewClientTLS(cargs TransportArgs, sargs TLSArgs) *http.Client {
	transport := NewTransport(cargs)
	debug.Assert(cargs.UseHTTPS, "no need to call NewClientTLS when not using HTTPS")

	// initialize TLS config and panic on err
	tlsConfig, err := NewTLS(sargs)
	cos.AssertNoErr(err)
	transport.TLSClientConfig = tlsConfig

	return &http.Client{Transport: transport, Timeout: cargs.Timeout}
}
