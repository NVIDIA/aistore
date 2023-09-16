//go:build nethttp

// Package transport provides streaming object-based transport over http for intra-cluster continuous
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

	// compare with ais/httpcommon.go
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
	return cmn.NewClient(cmn.TransportArgs{
		SndRcvBufSize:   tcpbuf,
		WriteBufferSize: wbuf,
		ReadBufferSize:  rbuf,
		UseHTTPS:        config.Net.HTTP.UseHTTPS,
		SkipVerify:      config.Net.HTTP.SkipVerify,
	})
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
		if verbose {
			nlog.Errorf("%s: Error [%v]", s, err)
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
