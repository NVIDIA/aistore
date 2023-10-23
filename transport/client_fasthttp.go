//go:build !nethttp

// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

	// compare with ais/httpcommon.go
	wbuf, rbuf := config.Net.HTTP.WriteBufferSize, config.Net.HTTP.ReadBufferSize
	if wbuf == 0 {
		wbuf = cmn.DefaultWriteBufferSize // fasthttp uses 4KB
	}
	if rbuf == 0 {
		rbuf = cmn.DefaultReadBufferSize // ditto
	}
	cl := &fasthttp.Client{
		Dial:            dialTimeout,
		ReadBufferSize:  rbuf,
		WriteBufferSize: wbuf,
	}
	if config.Net.HTTP.UseHTTPS {
		tlsConfig, err := cmn.NewTLS(cmn.TLSArgs{SkipVerify: config.Net.HTTP.SkipVerify})
		cos.AssertNoErr(err)
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
		if verbose {
			nlog.Errorf("%s: Error [%v]", s, err)
		}
		return
	}
	// handle response & cleanup
	resp.BodyWriteTo(io.Discard)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	if s.streamer.compressed() {
		s.streamer.resetCompression()
	}
	return
}
