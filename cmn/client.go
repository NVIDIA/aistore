// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/certloader"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	DfltDialupTimeout = 10 * time.Second
	DfltKeepaliveTCP  = 30 * time.Second
)

// [NOTE]
// net/http.DefaultTransport has the following defaults:
//
// - MaxIdleConns:          100,
// - MaxIdleConnsPerHost :  2 (via DefaultMaxIdleConnsPerHost)
// - IdleConnTimeout:       90 * time.Second,
// - WriteBufferSize:       4KB
// - ReadBufferSize:        4KB
//
// Following are the defaults we use instead:
const (
	DefaultMaxIdleConns        = 0               // unlimited (in re: `http.errTooManyIdle`)
	DefaultMaxIdleConnsPerHost = 32              // (http.errTooManyIdleHost)
	DefaultIdleConnTimeout     = 6 * time.Second // (Go default is 90s)
	DefaultWriteBufferSize     = 64 * cos.KiB
	DefaultReadBufferSize      = 64 * cos.KiB
	DefaultSndRcvBufferSize    = 128 * cos.KiB
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
		LowLatencyToS    bool
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
		defaultTransport = http.DefaultTransport.(*http.Transport)
		dialTimeout      = cos.NonZero(cargs.DialTimeout, DfltDialupTimeout)
	)

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: DfltKeepaliveTCP,
	}

	// NOTE: setsockopt when (SndRcvBufSize > 0 and/or LowLatencyToS)
	dialer.Control = cargs.clientControl()

	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		DisableCompression:    true, // NOTE: hardcoded - never used
	}
	transport.IdleConnTimeout = cos.NonZero(cargs.IdleConnTimeout, DefaultIdleConnTimeout)
	transport.MaxIdleConnsPerHost = cos.NonZero(cargs.IdleConnsPerHost, DefaultMaxIdleConnsPerHost)
	transport.MaxIdleConns = cos.NonZero(cargs.MaxIdleConns, DefaultMaxIdleConns)

	transport.WriteBufferSize = cos.NonZero(cargs.WriteBufferSize, DefaultWriteBufferSize)
	transport.ReadBufferSize = cos.NonZero(cargs.ReadBufferSize, DefaultReadBufferSize)

	// not used anymore
	if cargs.UseHTTPProxyEnv {
		transport.Proxy = defaultTransport.Proxy
	}
	return transport
}

func NewTLS(sargs TLSArgs, intra bool) (tlsConf *tls.Config, err error) {
	var pool *x509.CertPool
	if sargs.ClientCA != "" {
		cert, err := os.ReadFile(sargs.ClientCA)
		if err != nil {
			return nil, err
		}

		// from https://github.com/golang/go/blob/master/src/crypto/x509/cert_pool.go:
		// "On Unix systems other than macOS the environment variables SSL_CERT_FILE and
		// SSL_CERT_DIR can be used to override the system default locations for the SSL
		// certificate file and SSL certificate files directory, respectively. The
		// latter can be a colon-separated list."
		pool, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("client tls: failed to load system cert pool, err: %w", err)
		}
		if ok := pool.AppendCertsFromPEM(cert); !ok {
			return nil, fmt.Errorf("client tls: failed to append CA certs from PEM: %q", sargs.ClientCA)
		}
	}
	tlsConf = &tls.Config{RootCAs: pool, InsecureSkipVerify: sargs.SkipVerify}

	if sargs.Certificate == "" && sargs.Key == "" {
		return tlsConf, nil
	}

	// intra-cluster client
	if intra {
		tlsConf.GetClientCertificate, err = certloader.GetClientCert()
		return tlsConf, err
	}

	// external client
	var (
		cert tls.Certificate
		hint string
	)
	if cert, err = tls.LoadX509KeyPair(sargs.Certificate, sargs.Key); err == nil {
		tlsConf.Certificates = []tls.Certificate{cert}
		return tlsConf, nil
	}

	if os.IsNotExist(err) {
		hint = "\n(hint: check the two filenames for existence/accessibility)"
	}
	return nil, fmt.Errorf("client tls: failed to load public/private key pair: (%q, %q)%s", sargs.Certificate, sargs.Key, hint)
}

// TODO -- FIXME: this call must get cert file and key to be used for the `clientTLS`
func NewDefaultClients(timeout time.Duration) (clientH, clientTLS *http.Client) {
	clientH = NewClient(TransportArgs{Timeout: timeout})
	clientTLS = NewClientTLS(TransportArgs{Timeout: timeout}, TLSArgs{SkipVerify: true}, false /*intra-cluster*/)
	return
}

// NOTE: `NewTransport` (below) fills-in certain defaults
func NewClient(cargs TransportArgs) *http.Client {
	return &http.Client{Transport: NewTransport(cargs), Timeout: cargs.Timeout}
}

func NewIntraClientTLS(cargs TransportArgs, config *Config) *http.Client {
	return NewClientTLS(cargs, config.Net.HTTP.ToTLS(), true /*intra-cluster*/)
}

// https client (ditto)
func NewClientTLS(cargs TransportArgs, sargs TLSArgs, intra bool) *http.Client {
	transport := NewTransport(cargs)

	// initialize TLS config
	tlsConfig, err := NewTLS(sargs, intra)
	if err != nil {
		cos.ExitLog(err) // FATAL
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Transport: transport, Timeout: cargs.Timeout}
}

// EnvToTLS usage is limited to aisloader and tools
// NOTE that embedded intra-cluster clients utilize a similar method: `HTTPConf.ToTLS`
func EnvToTLS(sargs *TLSArgs) {
	if s := os.Getenv(env.AisClientCert); s != "" {
		sargs.Certificate = s
	}
	if s := os.Getenv(env.AisClientCertKey); s != "" {
		sargs.Key = s
	}
	if s := os.Getenv(env.AisClientCA); s != "" {
		// XXX This should be RootCA for clients
		// https://pkg.go.dev/crypto/tls
		sargs.ClientCA = s
	}
	if s := os.Getenv(env.AisSkipVerifyCrt); s != "" {
		sargs.SkipVerify = cos.IsParseBool(s)
	}
}
