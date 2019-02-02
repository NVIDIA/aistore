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
	ClientArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
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

func NewTransport(args ClientArgs) *http.Transport {
	var (
		dialTimeout      = args.DialTimeout
		idleConnsPerHost = args.IdleConnsPerHost
		defaultTransport = http.DefaultTransport.(*http.Transport)
	)

	if dialTimeout == 0 {
		dialTimeout = 30 * time.Second
	}

	transport := &http.Transport{
		Proxy: defaultTransport.Proxy,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
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

func NewClient(args ClientArgs) *http.Client {
	transport := NewTransport(args)
	client := &http.Client{
		Transport: transport,
		Timeout:   args.Timeout,
	}
	return client
}
