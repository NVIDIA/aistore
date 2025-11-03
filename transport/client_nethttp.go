//go:build nethttp

// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"context"
	"io"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

const ua = "aisnode/streams"

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func whichClient() string { return "net/http" }

// intra-cluster networking: net/http client
func NewIntraDataClient() (client *http.Client) {
	config := cmn.GCO.Get()
	httcfg := &config.Net.HTTP

	// (compare with cmn/client.go)
	cargs := cmn.TransportArgs{
		SndRcvBufSize:   cos.NonZero(config.Net.L4.SndRcvBufSize, int(cmn.DefaultSndRcvBufferSize)),
		WriteBufferSize: cos.NonZero(httcfg.WriteBufferSize, int(cmn.DefaultWriteBufferSize)),
		ReadBufferSize:  cos.NonZero(httcfg.ReadBufferSize, int(cmn.DefaultReadBufferSize)),
		IdleConnTimeout: cmn.DfltMaxIdleTimeout,
	}
	if config.Net.HTTP.UseHTTPS {
		client = cmn.NewClientTLS(cargs, config.Net.HTTP.ToTLS(), true /*intra-cluster*/) // streams
	} else {
		client = cmn.NewClient(cargs)
	}
	return
}

func (s *streamBase) doPlain(body io.Reader) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, s.dstURL, body)
	if err != nil {
		return err
	}
	return s._do(req)
}

func (s *streamBase) doCmpr(body io.Reader) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, s.dstURL, body)
	if err != nil {
		return err
	}
	req.Header.Set(apc.HdrCompress, apc.LZ4Compression)
	err = s._do(req)
	s.streamer.resetCompression()
	return err
}

func (s *streamBase) _do(req *http.Request) error {
	req.Header.Set(apc.HdrSessID, strconv.FormatInt(s.sessID, 10))
	req.Header.Set(apc.HdrSenderID, core.T.SID())
	req.Header.Set(cos.HdrUserAgent, ua)

	resp, err := s.client.Do(req)
	if err != nil {
		s.yelp(err)
		return err
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil {
		s.yelp(err)
	}
	return nil
}
