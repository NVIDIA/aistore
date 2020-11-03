// +build nethttp

// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
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
		UseHTTPS:        config.Net.HTTP.UseHTTPS,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
}

func (s *streamBase) do(body io.Reader) (err error) {
	var (
		request  *http.Request
		response *http.Response
	)
	if request, err = http.NewRequest(http.MethodPut, s.toURL, body); err != nil {
		return
	}
	if s.streamer.compressed() {
		request.Header.Set(cmn.HeaderCompress, cmn.LZ4Compression)
	}
	request.Header.Set(cmn.HeaderSessID, strconv.FormatInt(s.sessID, 10))

	response, err = s.client.Do(request)
	if err != nil {
		glog.Errorf("%s: Error [%v]", s, err)
		return
	}
	cmn.DrainReader(response.Body)
	response.Body.Close()
	if s.streamer.compressed() {
		s.streamer.resetCompression()
	}
	return
}
