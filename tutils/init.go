/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package tutils provides common low-level utilities for all dfcpub unit and integration tests
package tutils

import (
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/NVIDIA/dfcpub/memsys"
)

const (
	registerTimeout    = time.Minute * 2
	maxBodyErrorLength = 256
)

var (
	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 600 * time.Second,
		MaxIdleConnsPerHost: 100, // arbitrary number, to avoid connect: cannot assign requested address
	}
	BaseHTTPClient = &http.Client{}
	HTTPClient     = &http.Client{
		Timeout:   600 * time.Second,
		Transport: transport,
	}
	tr = &traceableTransport{
		transport: transport,
		tsBegin:   time.Now(),
	}
	trace = &httptrace.ClientTrace{
		GotConn:              tr.GotConn,
		WroteHeaders:         tr.WroteHeaders,
		WroteRequest:         tr.WroteRequest,
		GotFirstResponseByte: tr.GotFirstResponseByte,
	}
	tracedClient = &http.Client{Transport: tr}
	Mem2         *memsys.Mem2
)

func init() {
	Mem2 = &memsys.Mem2{Period: time.Minute * 2, Name: "ClientMem2"}
	_ = Mem2.Init(false /* ignore init-time errors */)
	go Mem2.Run()
}
