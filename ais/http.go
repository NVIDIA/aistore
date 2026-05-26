// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tracing"
)

type global struct {
	client struct {
		// config.Timeout
		cplane    *http.Client // intra-control, config.Timeout.CplaneOperation (keep-alive)
		maxkalive *http.Client // intra-control, config.Timeout.MaxKeepalive (bcastGroup and friends)

		// config.Client timeout
		control *http.Client // intra-control, config.Client.Timeout
		data    *http.Client // intra-data,    config.Client.TimeoutLong
	}
	netServ struct {
		pub      *netServer
		control  *netServer
		data     *netServer
		pubExtra []*netServer
	}
}

var g global

func handlePub(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range htverbs {
		g.netServ.pub.muxers[v].HandleFunc(path, handler)
		if !cos.IsLastB(path, '/') {
			g.netServ.pub.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func handleControl(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range htverbs {
		g.netServ.control.muxers[v].HandleFunc(path, handler)
		if !cos.IsLastB(path, '/') {
			g.netServ.control.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

func handleData(path string, handler func(http.ResponseWriter, *http.Request)) {
	for _, v := range htverbs {
		g.netServ.data.muxers[v].HandleFunc(path, handler)
		if !cos.IsLastB(path, '/') {
			g.netServ.data.muxers[v].HandleFunc(path+"/", handler)
		}
	}
}

const (
	defaultControlWriteBufferSize = 16 * cos.KiB
	defaultControlReadBufferSize  = 16 * cos.KiB
)

func initCtrlClient(config *cmn.Config, preferIPv6 bool) {
	cargs := cmn.TransportArgs{
		DialTimeout:      config.Client.Timeout.D(),
		IdleConnTimeout:  config.Net.HTTP.IdleConnTimeout.D(),
		IdleConnsPerHost: config.Net.HTTP.MaxIdleConnsPerHost,
		MaxIdleConns:     config.Net.HTTP.MaxIdleConns,
		WriteBufferSize:  defaultControlWriteBufferSize,
		ReadBufferSize:   defaultControlReadBufferSize,
		LowLatencyToS:    true,
		PreferIPv6:       preferIPv6,
	}

	// single shared transport => one connection pool across all three
	// intra-cluster control-plane clients (below)
	tr := cmn.NewTransport(cargs)
	if config.Net.HTTP.UseHTTPS {
		tlsConfig, err := cmn.NewTLS(config.Net.HTTP.ToTLS(), true /*intra*/)
		if err != nil {
			cos.ExitLog(err)
		}
		tr.TLSClientConfig = tlsConfig
	}

	// per-tier Timeout; Transport (and therefore connection pool) is shared
	g.client.control = &http.Client{Transport: tr, Timeout: config.Client.Timeout.D()}
	g.client.cplane = &http.Client{Transport: tr, Timeout: config.Timeout.CplaneOperation.D()}
	g.client.maxkalive = &http.Client{Transport: tr, Timeout: config.Timeout.MaxKeepalive.D()}
}

// wbuf/rbuf - when not configured use AIS defaults (to override the usual 4KB)
func initDataClient(config *cmn.Config, preferIPv6 bool) {
	wbuf, rbuf := config.Net.HTTP.WriteBufferSize, config.Net.HTTP.ReadBufferSize
	if wbuf == 0 {
		wbuf = cmn.DefaultWriteBufferSize
	}
	if rbuf == 0 {
		rbuf = cmn.DefaultReadBufferSize
	}
	cargs := cmn.TransportArgs{
		ClientTimeout:    config.Client.TimeoutLong.D(),
		WriteBufferSize:  wbuf,
		ReadBufferSize:   rbuf,
		IdleConnTimeout:  config.Net.HTTP.IdleConnTimeout.D(),
		IdleConnsPerHost: config.Net.HTTP.MaxIdleConnsPerHost,
		MaxIdleConns:     config.Net.HTTP.MaxIdleConns,
		PreferIPv6:       preferIPv6,
	}
	if config.Net.HTTP.UseHTTPS {
		g.client.data = cmn.NewIntraClientTLS(cargs, config)
	} else {
		g.client.data = cmn.NewClient(cargs)
	}

	// TODO:
	// tracing policy for intra-cluster HTTP (control and data, both)
	// should be unified; transport streams (and data movers) - are separate
	g.client.data = tracing.NewTraceableClient(g.client.data)
}

func shuthttp() {
	config := cmn.GCO.Get()
	g.netServ.pub.shutdown(config)
	for _, server := range g.netServ.pubExtra {
		server.shutdown(config)
	}
	if config.HostNet.UseIntraControl {
		g.netServ.control.shutdown(config)
	}
	if config.HostNet.UseIntraData {
		g.netServ.data.shutdown(config)
	}
}
