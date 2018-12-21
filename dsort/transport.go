/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/transport"
)

func newClient(conns int) *http.Client {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	client := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		MaxIdleConnsPerHost:   conns,
	}}
	return client
}

type StreamPool struct {
	roundRobinIdx int64
	Streams       []*transport.Stream
}

func NewStreamPool(streamCount int) *StreamPool {
	return &StreamPool{
		Streams: make([]*transport.Stream, 0, streamCount),
	}
}

func (sp *StreamPool) Get() *transport.Stream {
	idx := atomic.AddInt64(&sp.roundRobinIdx, 1)
	stream := sp.Streams[idx%int64(len(sp.Streams))]
	return stream
}

func (sp *StreamPool) Add(s *transport.Stream) {
	sp.Streams = append(sp.Streams, s)
}

func (sp *StreamPool) Stop() {
	for _, stream := range sp.Streams {
		stream.Fin()
	}
}

func NewStream(url string) *transport.Stream {
	extra := &transport.Extra{
		IdleTimeout: time.Minute * 5,
	}
	client := newClient(runtime.NumCPU() + 1)
	return transport.NewStream(client, url, extra)
}
