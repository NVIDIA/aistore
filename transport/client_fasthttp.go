// +build !nethttp

// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/valyala/fasthttp"
)

type Client interface {
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}

func whichClient() string { return "fasthttp" }

// intra-cluster networking: fasthttp client
func NewIntraDataClient() Client {
	config := cmn.GCO.Get()
	return &fasthttp.Client{
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
	}
}

func (s *Stream) do(body io.Reader) (err error) {
	// init request & response
	req, resp := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	req.Header.SetMethod(http.MethodPut)
	req.SetRequestURI(s.toURL)
	req.SetBodyStream(body, -1)
	if s.compressed() {
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
	if s.compressed() {
		s.lz4s.sgl.Reset()
		s.lz4s.zw.Reset(nil)
	}
	return
}
