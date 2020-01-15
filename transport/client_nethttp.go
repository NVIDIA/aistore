// +build nethttp

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
)

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func whichClient() string { return "net/http" }

// intra-cluster networking: net/http client
func NewIntraDataClient() *http.Client {
	config := cmn.GCO.Get()
	return cmn.NewClient(cmn.TransportArgs{
		SndRcvBufSize:   config.Net.L4.SndRcvBufSize,
		WriteBufferSize: config.Net.HTTP.WriteBufferSize,
		ReadBufferSize:  config.Net.HTTP.ReadBufferSize,
	})
}

func (s *Stream) do(body io.Reader) (err error) {
	// init request
	var (
		request  *http.Request
		response *http.Response
	)
	if request, err = http.NewRequest(http.MethodPut, s.toURL, body); err != nil {
		return
	}
	if s.compressed() {
		request.Header.Set(cmn.HeaderCompress, cmn.LZ4Compression)
	}
	request.Header.Set(cmn.HeaderSessID, strconv.FormatInt(s.sessID, 10))

	// do
	response, err = s.client.Do(request)
	if err != nil {
		glog.Errorf("%s: Error [%v]", s, err)
		return
	}

	// handle response & cleanup
	io.Copy(ioutil.Discard, response.Body)
	response.Body.Close()
	if s.compressed() {
		s.lz4s.sgl.Reset()
		s.lz4s.zw.Reset(nil)
	}
	return
}
