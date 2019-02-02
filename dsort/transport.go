/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/transport"
)

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
		IdleTimeout: time.Second * 30,
	}
	client := transport.NewDefaultClient()
	return transport.NewStream(client, url, extra)
}
