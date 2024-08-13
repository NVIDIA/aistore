// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	// assorted http(s) client options
	TransportArgs struct {
		DialTimeout      time.Duration
		Timeout          time.Duration
		IdleConnTimeout  time.Duration
		IdleConnsPerHost int
		MaxIdleConns     int
		SndRcvBufSize    int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPProxyEnv  bool
	}
	TLSArgs struct {
		ClientCA    string
		Certificate string
		Key         string
		SkipVerify  bool
	}
)

// {TransportArgs + defaults} => http.Transport for a variety of ais clients
// NOTE: TLS below, and separately
func NewTransport(cargs TransportArgs) *http.Transport {
	var (
		dialTimeout      = cargs.DialTimeout
		defaultTransport = http.DefaultTransport.(*http.Transport)
	)
	if dialTimeout == 0 {
		dialTimeout = 30 * time.Second
	}
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}
	// setsockopt when non-zero, otherwise use TCP defaults
	if cargs.SndRcvBufSize > 0 {
		dialer.Control = cargs.setSockOpt
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		IdleConnTimeout:       cargs.IdleConnTimeout,
		MaxIdleConnsPerHost:   cargs.IdleConnsPerHost,
		MaxIdleConns:          cargs.MaxIdleConns,
		WriteBufferSize:       cargs.WriteBufferSize,
		ReadBufferSize:        cargs.ReadBufferSize,
		DisableCompression:    true, // NOTE: hardcoded - never used
	}

	// apply global defaults
	if transport.MaxIdleConnsPerHost == 0 {
		transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	if transport.MaxIdleConns == 0 {
		transport.MaxIdleConns = DefaultMaxIdleConns
	}
	if transport.IdleConnTimeout == 0 {
		transport.IdleConnTimeout = DefaultIdleConnTimeout
	}
	if transport.WriteBufferSize == 0 {
		transport.WriteBufferSize = DefaultWriteBufferSize
	}
	if transport.ReadBufferSize == 0 {
		transport.ReadBufferSize = DefaultReadBufferSize
	}
	// not used anymore
	if cargs.UseHTTPProxyEnv {
		transport.Proxy = defaultTransport.Proxy
	}
	return transport
}

func NewTLS(sargs TLSArgs) (tlsConf *tls.Config, _ error) {
	var pool *x509.CertPool
	if sargs.ClientCA != "" {
		cert, err := os.ReadFile(sargs.ClientCA)
		if err != nil {
			return nil, err
		}
		pool = x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(cert); !ok {
			return nil, fmt.Errorf("client tls: failed to append CA certs from PEM: %q", sargs.ClientCA)
		}
	}
	tlsConf = &tls.Config{RootCAs: pool, InsecureSkipVerify: sargs.SkipVerify}
	if sargs.Certificate != "" {
		cert, err := tls.LoadX509KeyPair(sargs.Certificate, sargs.Key)
		if err != nil {
			var hint string
			if os.IsNotExist(err) {
				hint = "\n(hint: check the two filenames for existence/accessibility)"
			}
			return nil, fmt.Errorf("client tls: failed to load public/private key pair: (%q, %q)%s",
				sargs.Certificate, sargs.Key, hint)
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}
	return tlsConf, nil
}

func NewDefaultClients(timeout time.Duration) (clientH, clientTLS *http.Client) {
	clientH = NewClient(TransportArgs{Timeout: timeout})
	clientTLS = NewClientTLS(TransportArgs{Timeout: timeout}, TLSArgs{SkipVerify: true})
	return
}

// NOTE: `NewTransport` (below) fills-in certain defaults
func NewClient(cargs TransportArgs) *http.Client {
	return &http.Client{Transport: NewTransport(cargs), Timeout: cargs.Timeout}
}

func NewIntraClientTLS(cargs TransportArgs, config *Config) *http.Client {
	return NewClientTLS(cargs, config.Net.HTTP.ToTLS())
}

// https client (ditto)
func NewClientTLS(cargs TransportArgs, sargs TLSArgs) *http.Client {
	transport := NewTransport(cargs)

	// initialize TLS config
	tlsConfig, err := NewTLS(sargs)
	if err != nil {
		cos.ExitLog(err) // FATAL
	}
	transport.TLSClientConfig = tlsConfig

	return &http.Client{Transport: transport, Timeout: cargs.Timeout}
}

// see related: HTTPConf.ToTLS()
func EnvToTLS(sargs *TLSArgs) {
	if s := os.Getenv(env.AIS.Certificate); s != "" {
		sargs.Certificate = s
	}
	if s := os.Getenv(env.AIS.CertKey); s != "" {
		sargs.Key = s
	}
	if s := os.Getenv(env.AIS.ClientCA); s != "" {
		sargs.ClientCA = s
	}
	if s := os.Getenv(env.AIS.SkipVerifyCrt); s != "" {
		sargs.SkipVerify = cos.IsParseBool(s)
	}
}
