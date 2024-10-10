//go:build !nethttp

// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/valyala/fasthttp"
)

const ua = "aisnode/streams"

type Client interface {
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}

func whichClient() string { return "fasthttp" }

// overriding fasthttp default `const DefaultDialTimeout = 3 * time.Second`
func dialTimeout(addr string) (net.Conn, error) {
	return fasthttp.DialTimeout(addr, 10*time.Second)
}

// intra-cluster networking: fasthttp client
func NewIntraDataClient() Client {
	config := cmn.GCO.Get()
	httcfg := &config.Net.HTTP

	// (compare with ais/httpcommon.go)
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

func (s *streamBase) do(body io.Reader) (err error) {
	// init request & response
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(http.MethodPut)
	req.SetRequestURI(s.dstURL)
	req.SetBodyStream(body, -1)
	if s.streamer.compressed() {
		req.Header.Set(apc.HdrCompress, apc.LZ4Compression)
	}
	req.Header.Set(apc.HdrSessID, strconv.FormatInt(s.sessID, 10))
	req.Header.Set(cos.HdrUserAgent, ua)
	// do
	err = s.client.Do(req, resp)
	if err != nil {
		if cmn.Rom.FastV(5, cos.SmoduleTransport) {
			nlog.Errorln(s.String(), "err:", err)
		}
		return err
	}
	// handle response & cleanup
	resp.BodyWriteTo(io.Discard)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	if s.streamer.compressed() {
		s.streamer.resetCompression()
	}
	return nil
}
