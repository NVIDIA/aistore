//go:build !nethttp

// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"net"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"

	"github.com/valyala/fasthttp"
)

const ua = "aisnode/streams"

type Client interface {
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}

func whichClient() string { return "fasthttp" }

// overriding fasthttp default `const DefaultDialTimeout = 3 * time.Second`
func dialTimeout(addr string) (net.Conn, error) {
	return fasthttp.DialTimeout(addr, cmn.DfltDialupTimeout)
}

// intra-cluster networking: fasthttp client
func NewIntraDataClient() Client {
	config := cmn.GCO.Get()
	httcfg := &config.Net.HTTP

	// (compare with cmn/client.go)
	cl := &fasthttp.Client{
		Dial:            dialTimeout,
		ReadBufferSize:  cos.NonZero(httcfg.ReadBufferSize, int(cmn.DefaultReadBufferSize)),   // 4K
		WriteBufferSize: cos.NonZero(httcfg.WriteBufferSize, int(cmn.DefaultWriteBufferSize)), // ditto
	}
	if config.Net.HTTP.UseHTTPS {
		tlsConfig, err := cmn.NewTLS(config.Net.HTTP.ToTLS(), true /*intra-cluster*/) // streams
		if err != nil {
			cos.ExitLog(err)
		}
		cl.TLSConfig = tlsConfig
	}
	return cl
}

func (s *streamBase) doPlain(body io.Reader) (err error) {
	var (
		req  = fasthttp.AcquireRequest()
		resp = fasthttp.AcquireResponse()
	)
	err = s._do(body, req, resp)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return err
}

func (s *streamBase) doCmpr(body io.Reader) (err error) {
	var (
		req  = fasthttp.AcquireRequest()
		resp = fasthttp.AcquireResponse()
	)
	req.Header.Set(apc.HdrCompress, apc.LZ4Compression)

	err = s._do(body, req, resp)

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	s.streamer.resetCompression()
	return err
}

func (s *streamBase) _do(body io.Reader, req *fasthttp.Request, resp *fasthttp.Response) (err error) {
	req.Header.SetMethod(http.MethodPut)
	req.SetRequestURI(s.dstURL)
	req.SetBodyStream(body, -1)
	req.Header.Set(apc.HdrSessID, strconv.FormatInt(s.sessID, 10))
	req.Header.Set(apc.HdrSenderID, core.T.SID())
	req.Header.Set(cos.HdrUserAgent, ua)

	// do
	err = s.client.Do(req, resp)
	if err != nil {
		s.yelp(err)
		return err
	}

	// drain response & cleanup
	err = resp.BodyWriteTo(io.Discard)
	if err != nil {
		s.yelp(err)
	}
	return nil
}
