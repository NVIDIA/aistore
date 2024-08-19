//go:build nethttp

// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"io"
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const ua = "aisnode/streams"

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func whichClient() string { return "net/http" }

// intra-cluster networking: net/http client
func NewIntraDataClient() (client *http.Client) {
	config := cmn.GCO.Get()

	// compare with ais/hcommon.go
	wbuf, rbuf := config.Net.HTTP.WriteBufferSize, config.Net.HTTP.ReadBufferSize
	if wbuf == 0 {
		wbuf = cmn.DefaultWriteBufferSize
	}
	if rbuf == 0 {
		rbuf = cmn.DefaultReadBufferSize
	}
	tcpbuf := config.Net.L4.SndRcvBufSize
	if tcpbuf == 0 {
		tcpbuf = cmn.DefaultSendRecvBufferSize
	}
	cargs := cmn.TransportArgs{
		SndRcvBufSize:   tcpbuf,
		WriteBufferSize: wbuf,
		ReadBufferSize:  rbuf,
	}
	if config.Net.HTTP.UseHTTPS {
		client = cmn.NewClientTLS(cargs, config.Net.HTTP.ToTLS())
	} else {
		client = cmn.NewClient(cargs)
	}
	return
}

func (s *streamBase) do(body io.Reader) (err error) {
	var (
		request  *http.Request
		response *http.Response
	)
	if request, err = http.NewRequest(http.MethodPut, s.dstURL, body); err != nil {
		return
	}
	if s.streamer.compressed() {
		request.Header.Set(apc.HdrCompress, apc.LZ4Compression)
	}
	request.Header.Set(apc.HdrSessID, strconv.FormatInt(s.sessID, 10))
	request.Header.Set(cos.HdrUserAgent, ua)

	response, err = s.client.Do(request)
	if err != nil {
		if cmn.Rom.FastV(5, cos.SmoduleTransport) {
			nlog.Errorln(s.String(), "err:", err)
		}
		return
	}
	cos.DrainReader(response.Body)
	response.Body.Close()
	if s.streamer.compressed() {
		s.streamer.resetCompression()
	}
	return
}
