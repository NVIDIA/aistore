// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
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
// See also: docs/idle_connections.md
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

// TransportArgs.Egress: dial-time check that refuses destinations that IsBlockedEgressIP blocks.
type EgressPolicy uint8

const (
	EgressAny          EgressPolicy = iota // no check (default)
	EgressAllowPrivate                     // block restricted destinations while allowing private and ULA addresses
)

type (
	// assorted http(s) client options
	TransportArgs struct {
		DialTimeout      time.Duration
		ClientTimeout    time.Duration // http.Client.Timeout: end-to-end request
		IdleConnTimeout  time.Duration
		IdleConnsPerHost int
		MaxIdleConns     int
		SndRcvBufSize    int
		WriteBufferSize  int
		ReadBufferSize   int
		UseHTTPProxyEnv  bool
		LowLatencyToS    bool
		PreferIPv6       bool // usage: set-socket syscalls (cmn/network_unix)
		Egress           EgressPolicy
	}
	TLSArgs struct {
		ClientCA    string
		Certificate string
		Key         string
		SkipVerify  bool
		AltDNSName  string // intra-cluster only; see HTTPConf.ToTLSAlt
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

// accept a peer certificate valid for host or for name;
// nil roots means system roots
func verifyHostOrName(roots *x509.CertPool, host, name string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("client tls: no peer certificate")
		}
		leaf := cs.PeerCertificates[0]
		opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
		for _, c := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(c)
		}
		if _, err := leaf.Verify(opts); err != nil {
			return err
		}
		hostErr := leaf.VerifyHostname(host)
		if hostErr == nil {
			return nil
		}
		if err := leaf.VerifyHostname(name); err != nil {
			return errors.Join(hostErr, err)
		}
		return nil
	}
}

func (sargs *TLSArgs) useAlt(intra bool) bool {
	return intra && sargs.AltDNSName != "" && !sargs.SkipVerify
}

// complete client TLS handshake on conn;
// accept a peer certificate valid for the host in addr or for alt name
func handshakeAlt(ctx context.Context, conn net.Conn, addr string, conf *tls.Config, alt string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	conf = conf.Clone()
	// keep SNI, as http.Transport does without DialTLSContext
	if conf.ServerName == "" {
		conf.ServerName = host
	}
	conf.InsecureSkipVerify = true // replaced by VerifyConnection
	// VerifyConnection sees an empty server name for IP addresses (no SNI); bind the dialed host
	conf.VerifyConnection = verifyHostOrName(conf.RootCAs, host, alt)
	tc := tls.Client(conn, conf)
	if err := tc.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return tc, nil
}

// configure the transport as an HTTPS client
// - with alt name verification, later changes to tr.DialContext or tr.TLSClientConfig
// do not apply to HTTPS connections
func SetTLS(tr *http.Transport, sargs TLSArgs, intra bool) error {
	conf, err := NewTLS(sargs, intra)
	if err != nil {
		return err
	}
	tr.TLSClientConfig = conf
	if !sargs.useAlt(intra) {
		return nil
	}
	dial, timeout := tr.DialContext, tr.TLSHandshakeTimeout
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	tr.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return handshakeAlt(ctx, conn, addr, conf, sargs.AltDNSName)
	}
	return nil
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
		tlsConf.GetClientCertificate, err = certloader.Default.GetClientCert()
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

	if cos.IsNotExist(err) {
		hint = "\n(hint: check the two filenames for existence/accessibility)"
	}
	return nil, fmt.Errorf("client tls: failed to load public/private key pair: (%q, %q)%s", sargs.Certificate, sargs.Key, hint)
}

func NewDefaultClients(timeout time.Duration) (clientH, clientTLS *http.Client) {
	return NewClientPair(timeout, TLSArgs{})
}

// NewClientPair returns HTTP and HTTPS clients, configuring the latter with sargs.
func NewClientPair(timeout time.Duration, sargs TLSArgs) (clientH, clientTLS *http.Client) {
	clientH = NewClient(TransportArgs{ClientTimeout: timeout})
	clientTLS = NewClientTLS(TransportArgs{ClientTimeout: timeout}, sargs, false /*intra-cluster*/)
	return
}

// NewClient creates a plain-HTTP client with a new transport (and therefore a new
// connection pool). TransportArgs.ClientTimeout is assigned to http.Client.Timeout;
func NewClient(cargs TransportArgs) *http.Client {
	return &http.Client{Transport: NewTransport(cargs), Timeout: cargs.ClientTimeout}
}

// CloneClient returns a new *http.Client with an independent transport cloned from base
// (same TLS config, proxy settings, dial options, pool sizes) but with a different Timeout.
func CloneClient(base *http.Client, timeout time.Duration) *http.Client {
	bt := base.Transport.(*http.Transport)
	t := bt.Clone() // deep-copies TLS config, dial funcs, proxy — independent pool
	return &http.Client{Transport: t, Timeout: timeout}
}

// NewIntraClientTLS returns a client with its own transport and connection pool.
//
// Historically, AIS intra-control clients used this helper directly; currently
// the intra-control clients are constructed inline in ais/http.go with a shared transport
func NewIntraClientTLS(cargs TransportArgs, config *Config) *http.Client {
	return NewClientTLS(cargs, config.Net.HTTP.ToTLS(), true /*intra-cluster*/)
}

// NewClientTLS creates a new transport and therefore a new connection pool.
// TransportArgs.ClientTimeout becomes http.Client.Timeout.
func NewClientTLS(cargs TransportArgs, sargs TLSArgs, intra bool) *http.Client {
	transport := NewTransport(cargs)
	if err := SetTLS(transport, sargs, intra); err != nil {
		cos.ExitLog(err) // FATAL
	}
	return &http.Client{Transport: transport, Timeout: cargs.ClientTimeout}
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
