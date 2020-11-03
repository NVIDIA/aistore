// +build !nethttp

// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"crypto/tls"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/valyala/fasthttp"
)

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
	if !config.Net.HTTP.UseHTTPS {
		return &fasthttp.Client{
			Dial:            dialTimeout,
			ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
			WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		}
	}
	return &fasthttp.Client{
		Dial:            dialTimeout,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		TLSConfig:       &tls.Config{InsecureSkipVerify: config.Net.HTTP.SkipVerify},
	}
}

func (s *streamBase) do(body io.Reader) (err error) {
	// init request & response
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(http.MethodPut)
	req.SetRequestURI(s.toURL)
	req.SetBodyStream(body, -1)
	if s.streamer.compressed() {
		req.Header.Set(cmn.HeaderCompress, cmn.LZ4Compression)
	}
	req.Header.Set(cmn.HeaderSessID, strconv.FormatInt(s.sessID, 10))
	// do
	err = s.client.Do(req, resp)
	if err != nil {
		glog.Errorf("%s: Error [%v]", s, err)
		return
	}
	// handle response & cleanup
	resp.BodyWriteTo(ioutil.Discard)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	if s.streamer.compressed() {
		s.streamer.resetCompression()
	}
	return
}
