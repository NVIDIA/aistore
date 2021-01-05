// Package cmn provides common low-level types and utilities for all aistore projects
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
)

const (
	NetworkPublic       = "PUBLIC"
	NetworkIntraControl = "INTRA-CONTROL"
	NetworkIntraData    = "INTRA-DATA"
)

var KnownNetworks = []string{NetworkPublic, NetworkIntraControl, NetworkIntraData}

type (
	// Options to create a transport for HTTP client
	TransportArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
		SndRcvBufSize    int
		IdleConnsPerHost int
		MaxIdleConns     int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPS         bool
		UseHTTPProxyEnv  bool
		// For HTTPS mode only: if true, the client does not verify server's
		// certificate. It is useful for clusters with self-signed certificates.
		SkipVerify bool
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

	return ValidatePort(port)
}

func ValidatePort(port int) (int, error) {
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
		MaxIdleConns:          args.MaxIdleConns,
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
