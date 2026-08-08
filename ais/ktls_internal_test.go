// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tools/tassert"
)

const testktlsTimeout = 10 * time.Second

//
// helpers
//

type (
	// counts NewSessionTicket arrivals: crypto/tls calls Put only from
	// (*Conn).handleNewSessionTicket, which in turn is only reachable from a
	// post-handshake client Read
	ticketCounter struct {
		mu  sync.Mutex
		put int
	}
	// server side of one accepted connection
	testktlsServer struct {
		conn *ktlsTxConn
		err  error
	}
	testktlsState bool
)

func (state testktlsState) KTLSTxEnabled() bool { return bool(state) }
func (testktlsState) KTLSTxRetire(int64) bool   { return false }

func (tc *ticketCounter) Put(_ string, _ *tls.ClientSessionState) {
	tc.mu.Lock()
	tc.put++
	tc.mu.Unlock()
}

func (*ticketCounter) Get(string) (*tls.ClientSessionState, bool) { return nil, false }

func (tc *ticketCounter) count() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.put
}

func testktlsCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tassert.CheckFatal(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	tassert.CheckFatal(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func testktlsServerConf(t *testing.T) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{testktlsCert(t)}}
}

// NOTE: the client must _offer_ ALPN - server-side NextProtos alone negotiates
// nothing, and the corresponding assertion would pass vacuously
func testktlsClientConf(cache tls.ClientSessionCache) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed, loopback
		NextProtos:         []string{"http/1.1"},
		ClientSessionCache: cache,
	}
}

func testktlsNewListener(t *testing.T, raw net.Listener) *ktlsTxListener {
	t.Helper()

	l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
	tassert.CheckFatal(t, err)

	// Unit tests must remain independent of the host kernel and of the real
	// installer's implementation status.
	l.install = func(*net.TCPConn, *ktlsTxParams) (bool, error) { return false, nil }
	return l
}

// listener + one accept in the background
func testktlsListen(t *testing.T, l net.Listener) <-chan testktlsServer {
	t.Helper()

	ch := make(chan testktlsServer, 1)
	go func() {
		nc, err := l.Accept()
		if err != nil {
			ch <- testktlsServer{err: err}
			return
		}
		c, ok := nc.(*ktlsTxConn)
		if !ok {
			nc.Close()
			ch <- testktlsServer{err: errKTLSTxActive} // any non-nil; type is the failure
			return
		}
		// net/http through Go 1.26 arms via ConnectionState (see (*ktlsTxConn).init)
		c.ConnectionState()
		if c.initErr != nil {
			c.Close()
			ch <- testktlsServer{err: c.initErr}
			return
		}
		ch <- testktlsServer{conn: c}
	}()
	return ch
}

//
// tests
//

// using the fact that session tickets are the one thing that stdlib crypto/tls writes
// under the server application traffic epoch during the handshake
func TestKTLSTxSessionTickets(t *testing.T) {
	t.Run("control/tickets-enabled", func(t *testing.T) {
		cache := &ticketCounter{}
		conf := testktlsServerConf(t)
		conf.MinVersion, conf.MaxVersion = tls.VersionTLS13, tls.VersionTLS13

		raw, err := net.Listen("tcp", "127.0.0.1:0")
		tassert.CheckFatal(t, err)
		defer raw.Close()

		done := make(chan error, 1)
		go func() {
			nc, err := raw.Accept()
			if err != nil {
				done <- err
				return
			}
			sc := tls.Server(nc, conf)
			if err := sc.Handshake(); err != nil {
				sc.Close()
				done <- err
				return
			}
			_, err = sc.Write([]byte("x"))
			sc.Close()
			done <- err
		}()

		cc, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(cache))
		tassert.CheckFatal(t, err)

		// the ticket is processed here, not by Handshake
		_, err = io.ReadFull(cc, make([]byte, 1))
		tassert.CheckFatal(t, err)
		tassert.CheckError(t, cc.Close())
		tassert.CheckFatal(t, <-done)

		tassert.Errorf(t, cache.count() > 0,
			"control: expected the client to cache a session ticket, got %d - the control is broken",
			cache.count())
	})

	t.Run("ktls-tx-listener", func(t *testing.T) {
		cache := &ticketCounter{}

		raw, err := net.Listen("tcp", "127.0.0.1:0")
		tassert.CheckFatal(t, err)
		defer raw.Close()

		l := testktlsNewListener(t, raw)

		srvCh := testktlsListen(t, l)

		cc, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(cache))
		tassert.CheckFatal(t, err)

		srv := <-srvCh
		tassert.CheckFatal(t, srv.err)
		_, err = srv.conn.Write([]byte("x"))
		tassert.CheckFatal(t, err)

		_, err = io.ReadFull(cc, make([]byte, 1))
		tassert.CheckFatal(t, err)
		tassert.CheckError(t, cc.Close())
		tassert.CheckError(t, srv.conn.Close())

		tassert.Errorf(t, cache.count() == 0,
			"expected no session ticket, got %d: the server application record sequence "+
				"number would not start at zero", cache.count())
	})
}

func TestKTLSTxHandshake(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	defer raw.Close()

	l := testktlsNewListener(t, raw)

	srvCh := testktlsListen(t, l)

	cc, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(nil))
	tassert.CheckFatal(t, err)
	defer cc.Close()

	srv := <-srvCh
	tassert.CheckFatal(t, srv.err)
	defer srv.conn.Close()

	const payload = "aistore"
	_, err = srv.conn.Write([]byte(payload))
	tassert.CheckFatal(t, err)

	buf := make([]byte, len(payload))
	_, err = io.ReadFull(cc, buf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(buf) == payload, "expected %q, got %q", payload, buf)

	state := srv.conn.ConnectionState()
	tassert.Errorf(t, state.Version == tls.VersionTLS13, "expected TLS 1.3, got %#x", state.Version)
	tassert.Errorf(t, state.NegotiatedProtocol == "http/1.1", "expected http/1.1, got %q", state.NegotiatedProtocol)

	// the injected installer is a no-op: every connection stays on crypto/tls
	tassert.Errorf(t, !srv.conn.KTLSTxEnabled(), "armed with a no-op installer")
	tassert.Errorf(t, srv.conn.wire.nwritten.Load() > 0, "crypto/tls wrote nothing to the wire")

	// the effective per-connection config keeps the ticket prerequisite without
	// narrowing the caller's configured TLS version range
	tassert.Errorf(t, srv.conn.cfg.SessionTicketsDisabled, "SessionTicketsDisabled not set on the effective config")
	tassert.Errorf(t, srv.conn.cfg.MinVersion == 0 && srv.conn.cfg.MaxVersion == 0,
		"TLS version range unexpectedly changed to %#x-%#x", srv.conn.cfg.MinVersion, srv.conn.cfg.MaxVersion)

	// arm() wipes the key material on every path, including fallback
	tassert.Errorf(t, srv.conn.secrets.takeTLS13() == nil, "traffic secret not zeroed after arm")
}

func TestKTLSTxConcurrentInit(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	defer raw.Close()

	l := testktlsNewListener(t, raw)
	var installs atomic.Int32
	installing := make(chan struct{})
	release := make(chan struct{})
	l.install = func(*net.TCPConn, *ktlsTxParams) (bool, error) {
		if installs.Add(1) == 1 {
			close(installing)
		}
		<-release
		return false, nil
	}

	type serverResult struct {
		conn *ktlsTxConn
		err  error
	}
	serverCh := make(chan serverResult, 1)
	go func() {
		nc, err := l.Accept()
		if err != nil {
			serverCh <- serverResult{err: err}
			return
		}
		conn, ok := nc.(*ktlsTxConn)
		if !ok {
			nc.Close()
			serverCh <- serverResult{err: fmt.Errorf("expected *ktlsTxConn, got %T", nc)}
			return
		}
		serverCh <- serverResult{conn: conn}
	}()

	type clientResult struct {
		conn *tls.Conn
		err  error
	}
	clientCh := make(chan clientResult, 1)
	go func() {
		conn, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(nil))
		clientCh <- clientResult{conn: conn, err: err}
	}()

	srv := <-serverCh
	tassert.CheckFatal(t, srv.err)
	defer srv.conn.Close()

	const concurrency = 16
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	start := make(chan struct{})
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errCh <- srv.conn.HandshakeContext(t.Context())
		}()
	}
	close(start)

	select {
	case <-installing:
	case <-time.After(testktlsTimeout):
		close(release)
		t.Fatal("timed out waiting for concurrent kTLS initialization")
	}
	tassert.Errorf(t, installs.Load() == 1, "expected one installer call, got %d", installs.Load())
	close(release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(t, err)
	}
	tassert.Errorf(t, installs.Load() == 1, "expected one installer call, got %d", installs.Load())

	client := <-clientCh
	tassert.CheckFatal(t, client.err)
	defer client.conn.Close()

	const payload = "concurrent-init"
	_, err = srv.conn.Write([]byte(payload))
	tassert.CheckFatal(t, err)
	buf := make([]byte, len(payload))
	_, err = io.ReadFull(client.conn, buf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(buf) == payload, "expected %q, got %q", payload, buf)
}

func TestKTLSTxTLS12Handshake(t *testing.T) {
	serverConf := testktlsServerConf(t)
	serverConf.MinVersion, serverConf.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
	serverConf.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	defer raw.Close()

	l, err := newKTLSTxListener(raw, serverConf, testktlsTimeout, nil)
	tassert.CheckFatal(t, err)
	observed := make(chan ktlsTxParams, 1)
	l.install = func(_ *net.TCPConn, params *ktlsTxParams) (bool, error) {
		clone := *params
		clone.secret = bytes.Clone(params.secret)
		observed <- clone
		return false, nil
	}

	srvCh := testktlsListen(t, l)
	clientConf := testktlsClientConf(nil)
	clientConf.MinVersion, clientConf.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
	clientConf.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
	cc, err := tls.Dial("tcp", raw.Addr().String(), clientConf)
	tassert.CheckFatal(t, err)
	defer cc.Close()

	srv := <-srvCh
	tassert.CheckFatal(t, srv.err)
	defer srv.conn.Close()
	params := <-observed
	defer clear(params.secret)

	tassert.Errorf(t, params.version == tls.VersionTLS12, "expected TLS 1.2, got %#x", params.version)
	tassert.Errorf(t, params.cipherSuite == tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"unexpected cipher suite %#x", params.cipherSuite)
	// The observer sees the encrypted Finished record after ChangeCipherSpec.
	// Assert today's Go flow, but do not feed a fixed value to the installer.
	tassert.Errorf(t, params.recordSeq == [8]byte{7: 1}, "expected observed sequence 1, got %x", params.recordSeq)
	tassert.Errorf(t, params.clientRandom != [32]byte{}, "missing ClientHello.Random")
	tassert.Errorf(t, params.serverRandom != [32]byte{}, "missing ServerHello.Random")

	key, iv, supported, err := deriveTLS12KeyIV(params.cipherSuite, params.secret,
		params.clientRandom, params.serverRandom)
	tassert.CheckFatal(t, err)
	defer clear(key)
	defer clear(iv)
	tassert.Errorf(t, supported && len(key) == 16 && len(iv) == tls12IVSize,
		"failed to derive TLS 1.2 server write material")

	const payload = "aistore-tls12"
	_, err = srv.conn.Write([]byte(payload))
	tassert.CheckFatal(t, err)
	buf := make([]byte, len(payload))
	_, err = io.ReadFull(cc, buf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(buf) == payload, "expected %q, got %q", payload, buf)
	tassert.Errorf(t, !srv.conn.KTLSTxEnabled(), "armed with a no-op installer")

	master, _, ok := srv.conn.secrets.takeTLS12()
	clear(master)
	tassert.Errorf(t, !ok, "TLS 1.2 key material not zeroed after arm")
}

func TestKTLSTxKeyDerivation(t *testing.T) {
	tests := []struct {
		name        string
		secret      string
		key         string
		iv          string
		cipherSuite uint16
	}{
		{
			name:        "aes-128-gcm/rfc8448-server-application",
			cipherSuite: tls.TLS_AES_128_GCM_SHA256,
			secret:      "a11af9f05531f856ad47116b45a950328204b4f44bfb6b3a4b4f1f3fcb631643",
			key:         "9f02283b6c9c07efc26bb9f2ac92e356",
			iv:          "cf782b88dd83549aadf1e984",
		},
		{
			name:        "aes-256-gcm/sha384",
			cipherSuite: tls.TLS_AES_256_GCM_SHA384,
			secret: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
				"202122232425262728292a2b2c2d2e2f",
			key: "6877d022f1c61d24ebb7487c16752d9a4798e40431c75b39320e537c90e23225",
			iv:  "42822531a0fe88648fc09e9f",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			secret, err := hex.DecodeString(test.secret)
			tassert.CheckFatal(t, err)
			wantKey, err := hex.DecodeString(test.key)
			tassert.CheckFatal(t, err)
			wantIV, err := hex.DecodeString(test.iv)
			tassert.CheckFatal(t, err)

			key, iv, supported, err := deriveTLS13KeyIV(test.cipherSuite, secret)
			tassert.CheckFatal(t, err)
			defer clear(key)
			defer clear(iv)
			tassert.Errorf(t, supported, "cipher suite %#x is not supported", test.cipherSuite)
			tassert.Errorf(t, bytes.Equal(key, wantKey), "unexpected key for cipher %#x", test.cipherSuite)
			tassert.Errorf(t, bytes.Equal(iv, wantIV), "unexpected IV for cipher %#x", test.cipherSuite)
		})
	}

	key, iv, supported, err := deriveTLS13KeyIV(tls.TLS_CHACHA20_POLY1305_SHA256, make([]byte, 32))
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !supported, "ChaCha20-Poly1305 unexpectedly supported")
	tassert.Errorf(t, key == nil && iv == nil, "unsupported cipher returned key material")
}

func TestKTLSTxTLS12KeyDerivation(t *testing.T) {
	// Fixed independently against OpenSSL's TLS1-PRF KDF.
	master, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
		"202122232425262728292a2b2c2d2e2f")
	tassert.CheckFatal(t, err)
	clientBytes, err := hex.DecodeString("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
	tassert.CheckFatal(t, err)
	serverBytes, err := hex.DecodeString("404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f")
	tassert.CheckFatal(t, err)
	clientRandom, serverRandom := [32]byte{}, [32]byte{}
	copy(clientRandom[:], clientBytes)
	copy(serverRandom[:], serverBytes)

	tests := []struct {
		name        string
		key         string
		iv          string
		cipherSuite uint16
	}{
		{
			name:        "aes-128-gcm/sha256",
			cipherSuite: tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			key:         "617bfc73135fe88287599ae2278f1202",
			iv:          "3ffdfdf2",
		},
		{
			name:        "aes-256-gcm/sha384",
			cipherSuite: tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			key:         "2beee8b9885b18471b6d987d01c2e7fb36b5c2cdb42fd5a1ba07e906aeef53cf",
			iv:          "9e7af7e8",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantKey, err := hex.DecodeString(test.key)
			tassert.CheckFatal(t, err)
			wantIV, err := hex.DecodeString(test.iv)
			tassert.CheckFatal(t, err)

			key, iv, supported, err := deriveTLS12KeyIV(test.cipherSuite, master, clientRandom, serverRandom)
			tassert.CheckFatal(t, err)
			defer clear(key)
			defer clear(iv)
			tassert.Errorf(t, supported, "cipher suite %#x is not supported", test.cipherSuite)
			tassert.Errorf(t, bytes.Equal(key, wantKey), "unexpected key for cipher %#x", test.cipherSuite)
			tassert.Errorf(t, bytes.Equal(iv, wantIV), "unexpected IV for cipher %#x", test.cipherSuite)
		})
	}

	key, iv, supported, err := deriveTLS12KeyIV(tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		master, clientRandom, serverRandom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !supported, "ChaCha20-Poly1305 unexpectedly supported")
	tassert.Errorf(t, key == nil && iv == nil, "unsupported cipher returned key material")

	_, _, supported, err = deriveTLS12KeyIV(tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		master[:len(master)-1], clientRandom, serverRandom)
	tassert.Errorf(t, supported && err != nil, "invalid master-secret size accepted")
}

// Exercises the actual net/http integration rather than driving the wrapped
// connection directly: Serve -> ConnContext -> ConnectionState -> request.
func TestKTLSTxHTTPServerFallback(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	l := testktlsNewListener(t, raw)
	type observation struct {
		proto string
		err   error
		tls   bool
		pub   bool
		ktls  bool
	}
	observed := make(chan observation, 1)

	ns := &netServer{reqNet: reqNetPub}
	server := &http.Server{
		ConnContext: ns.connContext,
		TLSConfig:   l.tlsConfig,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "7")
			_, err := io.WriteString(w, "aistore")
			proto := ""
			if r.TLS != nil {
				proto = r.TLS.NegotiatedProtocol
			}
			observed <- observation{
				proto: proto,
				err:   err,
				tls:   r.TLS != nil,
				pub:   reqIsPub(r),
				ktls:  isKTLSTx(r.Context()),
			}
		}),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(l) }()

	transport := &http.Transport{
		TLSClientConfig:   testktlsClientConf(nil),
		ForceAttemptHTTP2: false,
	}
	client := &http.Client{Transport: transport, Timeout: testktlsTimeout}
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		if err := server.Close(); err != nil {
			t.Errorf("failed to close kTLS test server: %v", err)
		}
		select {
		case err := <-serveErr:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("unexpected kTLS test server error: %v", err)
			}
		case <-time.After(testktlsTimeout):
			t.Error("timed out waiting for kTLS test server to stop")
		}
	})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+raw.Addr().String(), http.NoBody)
	tassert.CheckFatal(t, err)
	resp, err := client.Do(req)
	tassert.CheckFatal(t, err)
	body, err := io.ReadAll(resp.Body)
	tassert.CheckFatal(t, err)
	tassert.CheckError(t, resp.Body.Close())
	tassert.Errorf(t, resp.StatusCode == http.StatusOK, "expected status 200, got %d", resp.StatusCode)
	tassert.Errorf(t, string(body) == "aistore", "expected %q, got %q", "aistore", body)

	got := <-observed
	tassert.CheckFatal(t, got.err)
	tassert.Errorf(t, got.tls, "missing Request.TLS")
	tassert.Errorf(t, got.pub, "request is not marked as public")
	tassert.Errorf(t, !got.ktls, "no-op installer armed kTLS")
	tassert.Errorf(t, got.proto == "http/1.1", "expected http/1.1, got %q", got.proto)
}

func TestKTLSTxReadHeaderTimeout(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	l := testktlsNewListener(t, raw)

	server := &http.Server{
		ReadHeaderTimeout: 50 * time.Millisecond,
		Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Error("handler reached with an incomplete request header")
		}),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(l) }()
	t.Cleanup(func() {
		_ = server.Close()
		select {
		case err := <-serveErr:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("unexpected kTLS test server error: %v", err)
			}
		case <-time.After(testktlsTimeout):
			t.Error("timed out waiting for kTLS test server to stop")
		}
	})

	client, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(nil))
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = client.Close() })
	_, err = io.WriteString(client, "GET / HTTP/1.1\r\nHost: localhost\r\n")
	tassert.CheckFatal(t, err)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = client.Read(make([]byte, 1))
	tassert.Fatalf(t, err != nil, "incomplete header unexpectedly produced a response")
	var netErr net.Error
	tassert.Errorf(t, !errors.As(err, &netErr) || !netErr.Timeout(),
		"ReadHeaderTimeout was cleared by lazy kTLS initialization: %v", err)
}

func TestKTLSTxSendfileRequest(t *testing.T) {
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://localhost/v1/objects/bck/obj", http.NoBody)
	tassert.CheckFatal(t, err)

	tassert.Errorf(t, canSendfileRequest(req, false), "plain HTTP request rejected")
	tassert.Errorf(t, !canSendfileRequest(nil, true), "nil HTTPS request accepted")
	tassert.Errorf(t, !canSendfileRequest(req, true), "unarmed HTTPS request accepted")

	ctx := context.WithValue(req.Context(), keyKTLSTx, testktlsState(true))
	armed := req.WithContext(ctx)
	tassert.Errorf(t, canSendfileRequest(armed, true), "armed kTLS HTTPS request rejected")

	ctx = context.WithValue(req.Context(), keyKTLSTx, testktlsState(false))
	fallback := req.WithContext(ctx)
	tassert.Errorf(t, !canSendfileRequest(fallback, true), "kTLS fallback request accepted")
}

func TestKTLSTxListenerRejects(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	defer raw.Close()

	_, err = newKTLSTxListener(raw, nil, testktlsTimeout, nil)
	tassert.Errorf(t, err != nil, "nil TLS config accepted")

	// crypto/tls swaps the entire per-connection config, dropping KeyLogWriter
	conf := testktlsServerConf(t)
	conf.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil }

	_, err = newKTLSTxListener(raw, conf, testktlsTimeout, nil)
	tassert.Errorf(t, err != nil, "GetConfigForClient accepted")
	if err != nil {
		tassert.Errorf(t, strings.Contains(err.Error(), "GetConfigForClient"),
			"unexpected error %q", err)
	}

	// the caller's config must not be mutated
	conf.GetConfigForClient = nil
	l, err := newKTLSTxListener(raw, conf, testktlsTimeout, nil)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !conf.SessionTicketsDisabled, "newKTLSTxListener mutated the caller's config")
	tassert.Errorf(t, l.tlsConfig.SessionTicketsDisabled, "template does not disable session tickets")
	tassert.Errorf(t, len(l.tlsConfig.NextProtos) == 1 && l.tlsConfig.NextProtos[0] == "http/1.1",
		"unexpected template NextProtos %v", l.tlsConfig.NextProtos)
	tassert.Errorf(t, l.install != nil, "nil installer")
}

func TestTLS12WireState(t *testing.T) {
	random := [32]byte{}
	for i := range random {
		random[i] = byte(i + 1)
	}
	serverHello := make([]byte, 4+2+len(random))
	serverHello[0] = tlsHandshakeTypeServerHello
	serverHello[3] = byte(2 + len(random))
	serverHello[4], serverHello[5] = 3, 3
	copy(serverHello[6:], random[:])

	wire := append(testktlsTLSRecord(tlsRecordTypeHandshake, serverHello),
		testktlsTLSRecord(tlsRecordTypeChangeCipherSpec, []byte{1})...)
	wire = append(wire, testktlsTLSRecord(tlsRecordTypeHandshake, []byte("finished"))...)
	wire = append(wire, testktlsTLSRecord(23, []byte("application"))...)

	var state tls12WireState
	steps := [...]int{1, 2, 7, 3, 11}
	for i := 0; len(wire) > 0; i++ {
		n := min(steps[i%len(steps)], len(wire))
		state.observe(wire[:n])
		wire = wire[n:]
	}
	serverRandom, recordSeq, ok := state.state()
	tassert.Errorf(t, ok, "failed to observe complete TLS 1.2 state")
	tassert.Errorf(t, serverRandom == random, "expected server random %x, got %x", random, serverRandom)
	tassert.Errorf(t, recordSeq == [8]byte{7: 2}, "expected two post-CCS records, got %x", recordSeq)

	t.Run("partial-record", func(t *testing.T) {
		var partial tls12WireState
		record := testktlsTLSRecord(tlsRecordTypeHandshake, serverHello)
		partial.observe(record[:len(record)-1])
		_, _, ok := partial.state()
		tassert.Errorf(t, !ok, "accepted a partial TLS record")
	})

	t.Run("malformed-ccs", func(t *testing.T) {
		var malformed tls12WireState
		malformed.observe(testktlsTLSRecord(tlsRecordTypeHandshake, serverHello))
		malformed.observe(testktlsTLSRecord(tlsRecordTypeChangeCipherSpec, []byte{2}))
		_, _, ok := malformed.state()
		tassert.Errorf(t, !ok, "accepted a malformed ChangeCipherSpec")
	})
}

func testktlsTLSRecord(recordType byte, payload []byte) []byte {
	record := make([]byte, tlsRecordHeaderSize+len(payload))
	record[0], record[1], record[2] = recordType, 3, 3
	record[3], record[4] = byte(len(payload)>>8), byte(len(payload))
	copy(record[tlsRecordHeaderSize:], payload)
	return record
}

// keylog parsing, independent of any handshake
func TestTrafficSecrets(t *testing.T) {
	const (
		hexSecret    = "0011223344556677889aabbccddeeff00112233445566778899aabbccddeeff0"
		other        = "ffeeddccbbaa99887766554433221100ffeeddccbbaa998877665544332211ff"
		clientRandom = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
		masterSecret = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
			"202122232425262728292a2b2c2d2e2f"
	)
	want, err := hex.DecodeString(hexSecret)
	tassert.CheckFatal(t, err)

	t.Run("split-writes", func(t *testing.T) {
		s := newTrafficSecrets()

		// one logical line delivered in three chunks, no trailing newline yet
		line := "SERVER_TRAFFIC_SECRET_0 abcd " + hexSecret
		s.Write([]byte(line[:10]))
		s.Write([]byte(line[10:30]))
		tassert.Errorf(t, s.takeTLS13() == nil, "secret extracted before end-of-line")

		s.Write([]byte(line[30:] + "\n"))
		secret := s.takeTLS13()
		tassert.Errorf(t, bytes.Equal(secret, want), "expected %x, got %x", want, secret)
		clear(secret)
	})

	t.Run("ignores-other-labels", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("CLIENT_TRAFFIC_SECRET_0 abcd " + other + "\n"))
		s.Write([]byte("CLIENT_HANDSHAKE_TRAFFIC_SECRET abcd " + other + "\n"))
		s.Write([]byte("SERVER_HANDSHAKE_TRAFFIC_SECRET abcd " + other + "\n"))
		tassert.Errorf(t, s.takeTLS13() == nil, "captured a non-application-traffic secret")

		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd " + hexSecret + "\n"))
		secret := s.takeTLS13()
		tassert.Errorf(t, bytes.Equal(secret, want), "expected %x, got %x", want, secret)
		clear(secret)
	})

	t.Run("malformed", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd\n"))      // too few fields
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 a b c d\n"))   // too many
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd zzzz\n")) // not hex
		tassert.Errorf(t, s.takeTLS13() == nil, "captured a malformed secret")
	})

	t.Run("tls12-client-random", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("CLIENT_RANDOM " + clientRandom + " " + masterSecret + "\n"))

		wantMaster, err := hex.DecodeString(masterSecret)
		tassert.CheckFatal(t, err)
		wantRandom, err := hex.DecodeString(clientRandom)
		tassert.CheckFatal(t, err)
		masterAlias := s.tls12Master
		master, random, ok := s.takeTLS12()
		tassert.Errorf(t, ok, "TLS 1.2 keylog line was not captured")
		tassert.Errorf(t, bytes.Equal(master, wantMaster), "expected master %x, got %x", wantMaster, master)
		tassert.Errorf(t, bytes.Equal(random[:], wantRandom), "expected client random %x, got %x", wantRandom, random)
		tassert.Errorf(t, s.tls12Master == nil && !s.haveTLS12, "takeTLS12 retained the master secret")
		tassert.Errorf(t, s.tls12ClientRandom == [32]byte{}, "takeTLS12 retained the client random")
		clear(master)
		tassert.Errorf(t, bytes.Equal(masterAlias, make([]byte, len(masterAlias))),
			"clearing the transferred master did not wipe the original allocation")
		master, _, ok = s.takeTLS12()
		clear(master)
		tassert.Errorf(t, !ok, "takeTLS12 returned the same key material twice")

		s.Write([]byte("CLIENT_RANDOM " + clientRandom + " " + masterSecret + "\n"))
		masterAlias = s.tls12Master
		s.zero()
		master, random, ok = s.takeTLS12()
		clear(master)
		tassert.Errorf(t, !ok, "zero left TLS 1.2 key material behind")
		tassert.Errorf(t, bytes.Equal(masterAlias, make([]byte, len(masterAlias))),
			"zero did not wipe the TLS 1.2 master secret")
		tassert.Errorf(t, random == [32]byte{} && s.tls12ClientRandom == [32]byte{},
			"zero did not wipe the TLS 1.2 client random")
	})

	t.Run("tls12-malformed", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("CLIENT_RANDOM abcd " + masterSecret + "\n"))
		s.Write([]byte("CLIENT_RANDOM " + clientRandom + " abcd\n"))
		master, _, ok := s.takeTLS12()
		clear(master)
		tassert.Errorf(t, !ok, "captured malformed TLS 1.2 key material")
	})

	t.Run("take-ownership", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd " + hexSecret + "\n"))

		// Retain aliases to prove takeTLS13 transfers the original allocation.
		serverApp := s.serverApp
		secret := s.takeTLS13()
		tassert.Errorf(t, bytes.Equal(secret, want), "expected %x, got %x", want, secret)
		tassert.Errorf(t, s.serverApp == nil, "takeTLS13 retained the secret")
		clear(secret)
		tassert.Errorf(t, bytes.Equal(serverApp, make([]byte, len(serverApp))),
			"clearing the transferred secret did not wipe the original allocation")
		tassert.Errorf(t, s.takeTLS13() == nil, "takeTLS13 returned the same secret twice")
	})

	t.Run("zero-stored", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd " + hexSecret + "\n"))

		// pending has length zero after its complete line was consumed.
		serverApp := s.serverApp
		pending := s.pending[:cap(s.pending)]

		s.zero()
		tassert.Errorf(t, s.takeTLS13() == nil, "zero() left key material behind")
		tassert.Errorf(t, bytes.Equal(serverApp, make([]byte, len(serverApp))),
			"zero() did not wipe the decoded server secret")
		tassert.Errorf(t, bytes.Equal(pending, make([]byte, len(pending))),
			"zero() did not wipe the consumed keylog line")
	})

	t.Run("last-write-wins", func(t *testing.T) {
		s := newTrafficSecrets()
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd " + other + "\n"))
		s.Write([]byte("SERVER_TRAFFIC_SECRET_0 abcd " + hexSecret + "\n"))
		secret := s.takeTLS13()
		tassert.Errorf(t, bytes.Equal(secret, want), "expected the later secret")
		clear(secret)
	})
}

// - openssl s_client is the only client here that can force TLS 1.3 connection to utilize a certain cipher;
// - stdlib crypto/tls documents TLS 1.3 cipher suites as "not configurable";
// - see `Config.CipherSuites` comment in https://github.com/golang/go/blob/master/src/crypto/tls/common.go
// - OpenSSL lets us exercise live AES-256 negotiation and the ChaCha20 fallback.
func TestKTLSTxOpenSSLCipherSuites(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not installed")
	}
	tests := []struct {
		name   string
		ossl   string
		suite  uint16
		keyLen int // zero => expected to fall back to crypto/tls
	}{
		{"aes-256-gcm/sha384", "TLS_AES_256_GCM_SHA384", tls.TLS_AES_256_GCM_SHA384, 32},
		{"aes-128-gcm/sha256", "TLS_AES_128_GCM_SHA256", tls.TLS_AES_128_GCM_SHA256, 16},
		{"chacha20-poly1305", "TLS_CHACHA20_POLY1305_SHA256", tls.TLS_CHACHA20_POLY1305_SHA256, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const payload = "TLS 1.3 cipher-suite verification payload"

			raw, err := net.Listen("tcp", "127.0.0.1:0")
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { _ = raw.Close() })

			l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
			tassert.CheckFatal(t, err)

			captured := make(chan ktlsTxParams, 1)
			l.install = func(_ *net.TCPConn, params *ktlsTxParams) (bool, error) {
				clone := *params
				clone.secret = append([]byte(nil), params.secret...)
				captured <- clone
				return false, nil // stay on crypto/tls; openssl must still decrypt
			}

			go func() {
				nc, err := l.Accept()
				if err != nil {
					return
				}
				conn := nc.(*ktlsTxConn)
				conn.ConnectionState()
				_, _ = conn.Write([]byte(payload))
				time.Sleep(100 * time.Millisecond)
				_ = conn.Close()
			}()

			out := testktlsOpenSSLClient(t, raw.Addr().String(), test.ossl, payload)

			var params ktlsTxParams
			select {
			case params = <-captured:
			case <-time.After(testktlsTimeout):
				t.Fatalf("installer was never called; openssl said: %s", out)
			}
			defer clear(params.secret)

			tassert.Errorf(t, params.version == tls.VersionTLS13,
				"expected TLS 1.3, got %#x", params.version)
			tassert.Errorf(t, params.cipherSuite == test.suite,
				"negotiated %#x, wanted %#x; openssl said: %s", params.cipherSuite, test.suite, out)
			tassert.Errorf(t, params.recordSeq == [8]byte{},
				"record sequence %v, want all-zero", params.recordSeq)

			key, iv, supported, err := deriveTLS13KeyIV(params.cipherSuite, params.secret)
			tassert.CheckFatal(t, err)
			defer clear(key)
			defer clear(iv)

			tassert.Errorf(t, bytes.Contains(out, []byte(payload)), "openssl did not receive the payload: %s", out)
			if test.keyLen == 0 {
				tassert.Errorf(t, !supported, "cipher %#x unexpectedly supported", params.cipherSuite)
				tassert.Errorf(t, key == nil && iv == nil, "unsupported cipher returned key material")
				return
			}
			tassert.Errorf(t, supported, "cipher %#x reported unsupported", params.cipherSuite)
			tassert.Errorf(t, len(key) == test.keyLen, "expected a %d-byte key, got %d", test.keyLen, len(key))
			tassert.Errorf(t, len(iv) == tls13IVSize, "expected a %d-byte IV, got %d", tls13IVSize, len(iv))
		})
	}
}

// run `openssl s_client` against addr with the given TLS 1.3 cipher suite and
// return whatever it printed. Its stdin is held open on purpose: at EOF s_client
// sends close_notify and exits before the server's application record arrives.
func testktlsOpenSSLClient(t *testing.T, addr, ciphersuite, await string) []byte {
	t.Helper()

	host, port, err := net.SplitHostPort(addr)
	tassert.CheckFatal(t, err)

	cmd := exec.CommandContext(t.Context(), "openssl", "s_client",
		"-connect", net.JoinHostPort(host, port),
		"-tls1_3", "-ciphersuites", ciphersuite,
		"-quiet", "-verify_quiet", "-no_ign_eof")

	stdin, err := cmd.StdinPipe()
	tassert.CheckFatal(t, err)

	out := &testktlsBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	tassert.CheckFatal(t, cmd.Start())

	deadline := time.Now().Add(testktlsTimeout)
	for time.Now().Before(deadline) && !bytes.Contains(out.bytes(), []byte(await)) {
		time.Sleep(20 * time.Millisecond)
	}
	_ = stdin.Close()
	_ = cmd.Process.Kill()
	_ = cmd.Wait()

	return out.bytes()
}

// OpenSSL's uppercase K command sends KeyUpdate(update_requested). The Go
// server must respond from its read path, which is deliberately fatal after
// the kernel has taken ownership of TLS TX.
func TestKTLSTxPeerKeyUpdate(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not installed")
	}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
	tassert.CheckFatal(t, err)
	l.install = func(*net.TCPConn, *ktlsTxParams) (bool, error) { return true, nil }

	type readyResult struct {
		conn *ktlsTxConn
		err  error
	}
	ready := make(chan readyResult, 1)
	readDone := make(chan error, 1)
	go func() {
		nc, err := l.Accept()
		if err != nil {
			ready <- readyResult{err: err}
			return
		}
		conn := nc.(*ktlsTxConn)
		if err := conn.HandshakeContext(t.Context()); err != nil {
			ready <- readyResult{err: err}
			_ = conn.tcp.Close()
			return
		}
		ready <- readyResult{conn: conn}
		_, err = conn.Read(make([]byte, 1))
		readDone <- err
	}()

	host, port, err := net.SplitHostPort(raw.Addr().String())
	tassert.CheckFatal(t, err)
	cmd := exec.CommandContext(t.Context(), "openssl", "s_client",
		"-connect", net.JoinHostPort(host, port),
		"-tls1_3", "-ciphersuites", "TLS_AES_128_GCM_SHA256",
		"-verify_quiet", "-no_ign_eof") // NOTE: -quiet nulls bio_c_out, where s_client prints KEYUPDATE
	stdin, err := cmd.StdinPipe()
	tassert.CheckFatal(t, err)
	out := &testktlsBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	tassert.CheckFatal(t, cmd.Start())
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	before := testktlsCounterSnapshot()
	var conn *ktlsTxConn
	select {
	case result := <-ready:
		tassert.CheckFatal(t, result.err)
		conn = result.conn
	case <-time.After(testktlsTimeout):
		t.Fatal("timed out waiting for OpenSSL handshake")
	}
	tassert.Fatalf(t, conn.KTLSTxEnabled(), "not armed")

	_, err = io.WriteString(stdin, "K\n")
	tassert.CheckFatal(t, err)
	select {
	case err = <-readDone:
		tassert.Errorf(t, err != nil, "peer-requested KeyUpdate left Read blocked without an error")
	case <-time.After(testktlsTimeout):
		t.Fatal("peer-requested KeyUpdate did not terminate the connection")
	}

	tassert.Errorf(t, bytes.Contains(out.bytes(), []byte("KEYUPDATE")),
		"OpenSSL did not recognize the KeyUpdate command: %s", out.bytes())
	tassert.Errorf(t, conn.txState.Load() == ktlsTxPoisoned,
		"state %d, wanted poisoned", conn.txState.Load())
	got := testktlsCounterSnapshot().sub(before)
	tassert.Errorf(t, got.poisoned == 1, "poisoned %d, wanted 1 (%s)", got.poisoned, got)
}

type testktlsBuf struct {
	mu  sync.Mutex
	buf []byte
}

func (b *testktlsBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	b.buf = append(b.buf, p...)
	b.mu.Unlock()
	return len(p), nil
}

func (b *testktlsBuf) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf...)
}

// end-to-end check of what arm() hands the kernel:
// - run TLS handshake;
// - decrypt the server's app record;
// - record sequence number zero.
func TestKTLSTxRecordDecrypt(t *testing.T) {
	const (
		payload = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut
labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.`
	)

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
	tassert.CheckFatal(t, err)

	captured := make(chan ktlsTxParams, 1)
	l.install = func(_ *net.TCPConn, params *ktlsTxParams) (bool, error) {
		clone := *params
		clone.secret = append([]byte(nil), params.secret...)
		captured <- clone
		return false, nil // crypto/tls must be the one encrypting what we decrypt
	}

	srvCh := testktlsListen(t, l)

	plain, err := net.Dial("tcp", raw.Addr().String())
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = plain.Close() })

	wire := &testktlsWiretap{Conn: plain}
	client := tls.Client(wire, testktlsClientConf(nil))
	tassert.CheckFatal(t, client.Handshake())

	srv := <-srvCh
	tassert.CheckFatal(t, srv.err)
	t.Cleanup(func() { _ = srv.conn.Close() })

	_, err = srv.conn.Write([]byte(payload))
	tassert.CheckFatal(t, err)

	buf := make([]byte, len(payload))
	_, err = io.ReadFull(client, buf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, string(buf) == payload, "client read %q", buf)

	params := <-captured
	defer clear(params.secret)
	tassert.Errorf(t, params.recordSeq == [8]byte{}, "record sequence %v, want all-zero", params.recordSeq)

	key, iv, supported, err := deriveTLS13KeyIV(params.cipherSuite, params.secret)
	tassert.CheckFatal(t, err)
	if !supported {
		t.Skipf("platform negotiated %s; record-decrypt test requires AES-GCM", tls.CipherSuiteName(params.cipherSuite))
	}
	defer clear(key)
	defer clear(iv)

	block, err := aes.NewCipher(key)
	tassert.CheckFatal(t, err)
	aead, err := cipher.NewGCM(block)
	tassert.CheckFatal(t, err)

	// the application record is the last thing the server sent, and under the
	// server application epoch it carries sequence number zero
	records := testktlsSplitRecords(t, wire.bytes())
	tassert.Fatalf(t, len(records) > 0, "no TLS records captured")
	last := records[len(records)-1]
	header, ciphertext := last[:testktlsRecordHdrSize], last[testktlsRecordHdrSize:]
	tassert.Errorf(t, header[0] == testktlsRecordAppData,
		"outer content type %#x, want application_data", header[0])

	// nonce = IV XOR the right-aligned big-endian sequence number (RFC 8446, 5.3)
	nonce := append([]byte(nil), iv...)
	for i, b := range params.recordSeq {
		nonce[len(nonce)-len(params.recordSeq)+i] ^= b
	}

	// AAD is the record header; failure here means the key, the IV or the
	// sequence number we would have handed the kernel is wrong
	plaintext, err := aead.Open(nil, nonce, ciphertext, header)
	tassert.CheckFatal(t, err)

	// TLS 1.3 inner plaintext: content || content_type || zero padding
	end := len(plaintext) - 1
	for end >= 0 && plaintext[end] == 0 {
		end--
	}
	tassert.Fatalf(t, end >= 0, "no inner content type")
	tassert.Errorf(t, plaintext[end] == testktlsRecordAppData,
		"inner content type %#x, want application_data", plaintext[end])
	tassert.Errorf(t, string(plaintext[:end]) == payload,
		"decrypted %q, want %q", plaintext[:end], payload)
}

const (
	testktlsRecordHdrSize = 5
	testktlsRecordAppData = 0x17
)

// records everything the peer sends, so that a test can inspect the ciphertext
// crypto/tls actually produced
type testktlsWiretap struct {
	net.Conn
	mu  sync.Mutex
	buf []byte
}

func (w *testktlsWiretap) Read(p []byte) (int, error) {
	n, err := w.Conn.Read(p)
	if n > 0 {
		w.mu.Lock()
		w.buf = append(w.buf, p[:n]...)
		w.mu.Unlock()
	}
	return n, err
}

func (w *testktlsWiretap) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf...)
}

func testktlsSplitRecords(t *testing.T, stream []byte) [][]byte {
	t.Helper()

	var records [][]byte
	for len(stream) >= testktlsRecordHdrSize {
		size := int(binary.BigEndian.Uint16(stream[3:testktlsRecordHdrSize]))
		tassert.Fatalf(t, len(stream) >= testktlsRecordHdrSize+size,
			"truncated record: have %d, need %d", len(stream), testktlsRecordHdrSize+size)
		records = append(records, stream[:testktlsRecordHdrSize+size])
		stream = stream[testktlsRecordHdrSize+size:]
	}
	tassert.Fatalf(t, len(stream) == 0, "%d trailing bytes", len(stream))
	return records
}

func TestKTLSTxCounters(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var counters ktlsTxCounters
		counters.armed.Store(3)
		counters.failed.Store(1)
		want := "ktls-tx[attempted=4 armed=3 skipped=0 unsupported=0 failed=1 poisoned=0 exhausted=0]"
		tassert.Errorf(t, counters.String() == want, "expected %q, got %q", want, counters.String())
	})

	// each installer outcome lands in exactly one bucket
	tests := []struct {
		name    string
		install ktlsTxInstaller
		armed   int64
		unsup   int64
		failed  int64
	}{
		{
			name:    "armed",
			install: func(*net.TCPConn, *ktlsTxParams) (bool, error) { return true, nil },
			armed:   1,
		},
		{
			name:    "unsupported",
			install: func(*net.TCPConn, *ktlsTxParams) (bool, error) { return false, nil },
			unsup:   1,
		},
		{
			name:    "failed",
			install: func(*net.TCPConn, *ktlsTxParams) (bool, error) { return false, errors.New("boom") },
			failed:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := testktlsCounterSnapshot()

			conn := testktlsHandshake(t, test.install)
			tassert.Errorf(t, conn.KTLSTxEnabled() == (test.armed == 1),
				"armed state %v, wanted %v", conn.KTLSTxEnabled(), test.armed == 1)

			got := testktlsCounterSnapshot().sub(before)
			tassert.Errorf(t, got.armed == test.armed, "armed %d, wanted %d (%s)", got.armed, test.armed, got)
			tassert.Errorf(t, got.unsupported == test.unsup, "unsupported %d, wanted %d (%s)", got.unsupported, test.unsup, got)
			tassert.Errorf(t, got.failed == test.failed, "failed %d, wanted %d (%s)", got.failed, test.failed, got)
			tassert.Errorf(t, got.skipped == 0, "skipped %d, wanted 0 (%s)", got.skipped, got)
			tassert.Errorf(t, got.poisoned == 0, "poisoned %d, wanted 0 (%s)", got.poisoned, got)
			tassert.Errorf(t, got.exhausted == 0, "exhausted %d, wanted 0 (%s)", got.exhausted, got)
			tassert.Errorf(t, got.total() == 1, "expected exactly one outcome, got %s", got)

			t.Logf("delta: %s; cumulative: %+v", got, &ktlsTxCnt)
		})
	}

	// arm() bailing on its own is a distinct bucket: session tickets left on
	// would make the TLS 1.3 record sequence number non-zero
	t.Run("skipped/session-tickets", func(t *testing.T) {
		before := testktlsCounterSnapshot()

		tmpl := testktlsServerConf(t)
		tmpl.NextProtos = []string{"http/1.1"}
		tmpl.SessionTicketsDisabled = false // bypasses newKTLSTxListener on purpose

		conn := testktlsHandshakeConf(t, tmpl,
			func(*net.TCPConn, *ktlsTxParams) (bool, error) {
				t.Error("installer reached with session tickets enabled")
				return false, nil
			})
		tassert.Errorf(t, !conn.KTLSTxEnabled(), "armed with session tickets enabled")

		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.skipped == 1, "skipped %d, wanted 1 (%s)", got.skipped, got)
		tassert.Errorf(t, got.armed == 0, "armed despite session tickets (%s)", got)
		tassert.Errorf(t, got.poisoned == 0, "poisoned %d, wanted 0 (%s)", got.poisoned, got)
		tassert.Errorf(t, got.exhausted == 0, "exhausted %d, wanted 0 (%s)", got.exhausted, got)
		tassert.Errorf(t, got.total() == 1, "expected exactly one outcome, got %s", got)

		t.Logf("delta: %s; cumulative: %+v", got, &ktlsTxCnt)
	})
}

type testktlsCounterValues struct {
	armed       int64
	skipped     int64
	unsupported int64
	failed      int64
	poisoned    int64
	exhausted   int64
}

func testktlsCounterSnapshot() testktlsCounterValues {
	return testktlsCounterValues{
		armed:       ktlsTxCnt.armed.Load(),
		skipped:     ktlsTxCnt.skipped.Load(),
		unsupported: ktlsTxCnt.unsupported.Load(),
		failed:      ktlsTxCnt.failed.Load(),
		poisoned:    ktlsTxCnt.poisoned.Load(),
		exhausted:   ktlsTxCnt.exhausted.Load(),
	}
}

func (c testktlsCounterValues) sub(prev testktlsCounterValues) testktlsCounterValues {
	c.armed -= prev.armed
	c.skipped -= prev.skipped
	c.unsupported -= prev.unsupported
	c.failed -= prev.failed
	c.poisoned -= prev.poisoned
	c.exhausted -= prev.exhausted
	return c
}

func (c testktlsCounterValues) total() int64 {
	return c.armed + c.skipped + c.unsupported + c.failed
}

func (c testktlsCounterValues) String() string {
	return fmt.Sprintf("armed=%d skipped=%d unsupported=%d failed=%d poisoned=%d exhausted=%d",
		c.armed, c.skipped, c.unsupported, c.failed, c.poisoned, c.exhausted)
}

// Post-arm the kernel owns transmit, so any crypto/tls write - a TLS 1.3
// KeyUpdate response, an alert - must end the connection.
func TestKTLSTxPoisoned(t *testing.T) {
	arm := func(*net.TCPConn, *ktlsTxParams) (bool, error) { return true, nil }

	t.Run("crypto-tls-write-poisons", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		tassert.Fatalf(t, conn.KTLSTxEnabled(), "not armed")

		// stands in for crypto/tls writing on the read path
		n, err := conn.wire.Write([]byte("keyupdate"))
		tassert.Errorf(t, n == 0, "wire write returned %d bytes", n)
		tassert.Errorf(t, errors.Is(err, errKTLSTxActive), "wire write returned %v", err)

		// fatal on purpose: an unpoisoned connection makes the reads below block
		tassert.Fatalf(t, conn.txState.Load() == ktlsTxPoisoned, "state %d, wanted poisoned", conn.txState.Load())
		tassert.Errorf(t, !conn.KTLSTxEnabled(), "a poisoned connection still reports kTLS enabled")
		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.poisoned == 1, "poisoned %d, wanted 1 (%s)", got.poisoned, got)

		// ...and every subsequent application operation refuses
		_, err = conn.Read(make([]byte, 1))
		tassert.Errorf(t, errors.Is(err, errKTLSTxPoisoned), "Read returned %v", err)
		_, err = conn.Write([]byte("x"))
		tassert.Errorf(t, errors.Is(err, errKTLSTxPoisoned), "Write returned %v", err)
		_, err = conn.ReadFrom(strings.NewReader("x"))
		tassert.Errorf(t, errors.Is(err, errKTLSTxPoisoned), "ReadFrom returned %v", err)
		_, err = conn.wire.ReadFrom(strings.NewReader("x"))
		tassert.Errorf(t, errors.Is(err, errKTLSTxActive), "wire ReadFrom returned %v", err)
		err = conn.CloseWrite()
		tassert.Errorf(t, errors.Is(err, errKTLSTxPoisoned), "CloseWrite returned %v", err)
		err = conn.Close()
		tassert.Errorf(t, errors.Is(err, errKTLSTxPoisoned), "Close returned %v", err)
	})

	t.Run("poison-is-counted-once-per-conn", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		for range 3 {
			_, _ = conn.wire.Write([]byte("alert"))
		}
		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.poisoned == 1,
			"poisoned %d, wanted 1 - the transition must be a one-way CAS", got.poisoned)
	})

	t.Run("unarmed-writes-pass-through", func(t *testing.T) {
		conn := testktlsHandshake(t, func(*net.TCPConn, *ktlsTxParams) (bool, error) { return false, nil })
		tassert.Errorf(t, conn.txState.Load() == ktlsTxUnarmed, "state %d, wanted unarmed", conn.txState.Load())

		before := conn.wire.nwritten.Load()
		n, err := conn.Write([]byte("hello"))
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == len("hello"), "wrote %d bytes, wanted %d", n, len("hello"))
		tassert.Errorf(t, conn.wire.nwritten.Load() > before, "application write did not reach the TLS wire")
	})
}

func TestKTLSTxBudget(t *testing.T) {
	arm := func(*net.TCPConn, *ktlsTxParams) (bool, error) { return true, nil }

	t.Run("write", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		conn.txMaxBytes = 10

		n, err := conn.Write([]byte("hello"))
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == 5, "wrote %d bytes, wanted 5", n)
		tassert.Errorf(t, conn.txBytes.Load() == 5, "counted %d bytes, wanted 5", conn.txBytes.Load())

		// Reaching the exact limit completes. Retiring here could close a
		// concurrently reserved write that has not reached the socket yet.
		n, err = conn.Write([]byte("world"))
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == 5, "wrote %d bytes, wanted 5", n)
		tassert.Errorf(t, conn.txBytes.Load() == 10, "counted %d bytes, wanted 10", conn.txBytes.Load())
		tassert.Errorf(t, conn.txState.Load() == ktlsTxArmed, "state %d, wanted armed", conn.txState.Load())

		n, err = conn.Write([]byte("!"))
		tassert.Errorf(t, n == 0, "wrote %d bytes past the budget", n)
		tassert.Errorf(t, errors.Is(err, errKTLSTxExhausted), "expected exhausted error, got %v", err)
		tassert.Errorf(t, conn.txBytes.Load() == 10, "count changed to %d after rejected write", conn.txBytes.Load())
		tassert.Errorf(t, conn.txState.Load() == ktlsTxExhausted, "state %d, wanted exhausted", conn.txState.Load())
		tassert.Errorf(t, !conn.KTLSTxEnabled(), "exhausted connection still reports kTLS enabled")

		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.exhausted == 1, "exhausted %d, wanted 1 (%s)", got.exhausted, got)
		tassert.Errorf(t, got.total() == 1, "budget changed arm outcomes: %s", got)
	})

	// a transfer that crosses the limit completes (bounded overshoot); the next one trips the backstop
	t.Run("oversized-transmit-completes-once", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		conn.txMaxBytes = 10

		payload := make([]byte, 11)
		n, err := conn.Write(payload)
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == 11, "wrote %d bytes, wanted 11 (must not truncate)", n)
		tassert.Errorf(t, conn.txBytes.Load() == 11, "counted %d bytes, wanted 11", conn.txBytes.Load())
		tassert.Errorf(t, conn.txState.Load() == ktlsTxArmed, "state %d, wanted armed", conn.txState.Load())
		tassert.Errorf(t, conn.KTLSTxRemaining() == 0, "remaining %d, wanted 0", conn.KTLSTxRemaining())

		n, err = conn.Write([]byte("!"))
		tassert.Errorf(t, n == 0, "wrote %d bytes past the budget", n)
		tassert.Errorf(t, errors.Is(err, errKTLSTxExhausted), "expected exhausted error, got %v", err)
		tassert.Errorf(t, conn.txState.Load() == ktlsTxExhausted, "state %d, wanted exhausted", conn.txState.Load())

		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.exhausted == 1, "exhausted %d, wanted 1 (%s)", got.exhausted, got)
	})

	// the transmit path retires the connection before the backstop above can fire
	t.Run("retire-before-crossing", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		conn.txMaxBytes = ktlsTxHeadroom + 10

		ctx := context.WithValue(t.Context(), keyKTLSTx, ktlsTxState(conn))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://127.0.0.1/v1/objects/nnn/o", http.NoBody)
		tassert.CheckFatal(t, err)

		whdr := http.Header{}
		ktlsTxRetire(req, whdr, 9)
		tassert.Errorf(t, whdr.Get(hdrConnection) == "", "retired early: %q", whdr.Get(hdrConnection))

		ktlsTxRetire(req, whdr, 10)
		tassert.Errorf(t, whdr.Get(hdrConnection) == hdrConnectionClose,
			"%q, wanted %q", whdr.Get(hdrConnection), hdrConnectionClose)
		tassert.Errorf(t, conn.txRetiring.Load(), "connection was not marked retiring")

		// net/http writes a response prefix before ReadFrom/sendfile. Both
		// operations belong to the same retiring response and must complete.
		conn.txBytes.Store(conn.txMaxBytes - 1)
		n, err := conn.Write(make([]byte, 2))
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == 2, "response prefix wrote %d bytes, wanted 2", n)
		body := &io.LimitedReader{R: strings.NewReader("aistore"), N: 7}
		n64, err := conn.ReadFrom(body)
		tassert.CheckError(t, err)
		tassert.Errorf(t, n64 == 7, "response body wrote %d bytes, wanted 7", n64)
		tassert.Errorf(t, conn.KTLSTxRemaining() == 0, "remaining %d, wanted 0", conn.KTLSTxRemaining())

		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.exhausted == 0, "retiring response exhausted the connection (%s)", got)

		// unarmed: no hint, no budget
		unarmed := testktlsHandshake(t, func(*net.TCPConn, *ktlsTxParams) (bool, error) { return false, nil })
		tassert.Errorf(t, unarmed.KTLSTxRemaining() == 0, "unarmed remaining %d", unarmed.KTLSTxRemaining())
		whdr = http.Header{}
		ktlsTxRetire(req.WithContext(context.WithValue(t.Context(), keyKTLSTx, ktlsTxState(unarmed))), whdr, 1<<40)
		tassert.Errorf(t, whdr.Get(hdrConnection) == "", "unarmed conn retired: %q", whdr.Get(hdrConnection))
	})

	t.Run("read-from", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		conn := testktlsHandshake(t, arm)
		conn.txMaxBytes = 7

		first := &io.LimitedReader{R: strings.NewReader("aistore"), N: 7}
		n, err := conn.ReadFrom(first)
		tassert.CheckError(t, err)
		tassert.Errorf(t, n == 7, "ReadFrom wrote %d bytes, wanted 7", n)
		tassert.Errorf(t, conn.txBytes.Load() == 7, "counted %d bytes, wanted 7", conn.txBytes.Load())
		tassert.Errorf(t, conn.txState.Load() == ktlsTxArmed, "state %d, wanted armed", conn.txState.Load())

		second := &io.LimitedReader{R: strings.NewReader("ktls"), N: 4}
		n, err = conn.ReadFrom(second)
		tassert.Errorf(t, n == 0, "ReadFrom wrote %d bytes past the budget", n)
		tassert.Errorf(t, errors.Is(err, errKTLSTxExhausted), "expected exhausted error, got %v", err)
		tassert.Errorf(t, second.N == 4, "rejected ReadFrom consumed %d bytes", 4-second.N)

		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.exhausted == 1, "exhausted %d, wanted 1 (%s)", got.exhausted, got)
		tassert.Errorf(t, got.total() == 1, "budget changed arm outcomes: %s", got)
	})
}

func TestKTLSTxArmCloseSerialized(t *testing.T) {
	t.Run("arm-first", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		installing := make(chan struct{})
		release := make(chan struct{})
		var (
			installs    atomic.Int32
			releaseOnce sync.Once
		)
		releaseInstall := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseInstall()

		conn, client := testktlsConnPair(t, func(*net.TCPConn, *ktlsTxParams) (bool, error) {
			installs.Add(1)
			close(installing)
			<-release
			return true, nil
		})
		handshake := testktlsStartHandshake(t, conn, client)
		select {
		case <-installing:
		case <-time.After(testktlsTimeout):
			t.Fatal("timed out waiting for kTLS installer")
		}
		if conn.txMu.TryLock() {
			conn.txMu.Unlock()
			t.Fatal("txMu does not span the installer")
		}

		closeStarted := make(chan struct{})
		closed := make(chan error, 1)
		go func() {
			close(closeStarted)
			closed <- conn.Close()
		}()
		<-closeStarted
		select {
		case err := <-closed:
			t.Fatalf("Close returned during installation: %v", err)
		case <-time.After(10 * time.Millisecond):
		}

		releaseInstall()
		select {
		case <-closed:
		case <-time.After(testktlsTimeout):
			t.Fatal("Close blocked after installation completed")
		}
		testktlsWaitHandshake(t, handshake)

		tassert.Errorf(t, installs.Load() == 1, "installer called %d times", installs.Load())
		tassert.Errorf(t, conn.txClosed, "transmit close was not recorded")
		tassert.Errorf(t, conn.txState.Load() == ktlsTxArmed, "state %d, wanted armed", conn.txState.Load())
		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.armed == 1, "armed %d, wanted 1 (%s)", got.armed, got)
		tassert.Errorf(t, got.skipped == 0, "skipped %d, wanted 0 (%s)", got.skipped, got)
		tassert.Errorf(t, got.poisoned == 0, "poisoned %d, wanted 0 (%s)", got.poisoned, got)
		tassert.Errorf(t, got.exhausted == 0, "exhausted %d, wanted 0 (%s)", got.exhausted, got)
		tassert.Errorf(t, got.total() == 1, "expected exactly one outcome, got %s", got)
	})

	t.Run("close-first", func(t *testing.T) {
		before := testktlsCounterSnapshot()
		var installs atomic.Int32
		conn, client := testktlsConnPair(t, func(*net.TCPConn, *ktlsTxParams) (bool, error) {
			installs.Add(1)
			return true, nil
		})

		// Claim transmit shutdown without closing the test socket: a real Close
		// would abort the handshake before arm() can exercise the latch.
		state := conn.beginTxClose()
		tassert.Errorf(t, state == ktlsTxUnarmed, "close observed state %d, wanted unarmed", state)
		testktlsWaitHandshake(t, testktlsStartHandshake(t, conn, client))

		tassert.Errorf(t, installs.Load() == 0, "installer called %d times after transmit close", installs.Load())
		tassert.Errorf(t, conn.txClosed, "transmit close was not recorded")
		tassert.Errorf(t, conn.txState.Load() == ktlsTxUnarmed, "state %d, wanted unarmed", conn.txState.Load())
		got := testktlsCounterSnapshot().sub(before)
		tassert.Errorf(t, got.skipped == 1, "skipped %d, wanted 1 (%s)", got.skipped, got)
		tassert.Errorf(t, got.armed == 0, "armed %d, wanted 0 (%s)", got.armed, got)
		tassert.Errorf(t, got.poisoned == 0, "poisoned %d, wanted 0 (%s)", got.poisoned, got)
		tassert.Errorf(t, got.exhausted == 0, "exhausted %d, wanted 0 (%s)", got.exhausted, got)
		tassert.Errorf(t, got.total() == 1, "expected exactly one outcome, got %s", got)
	})
}

// one handshake through a ktlsTxListener with the given installer
func testktlsHandshake(t *testing.T, install ktlsTxInstaller) *ktlsTxConn {
	t.Helper()
	return testktlsHandshakeConf(t, nil, install)
}

// Construct both ends without starting the TLS handshake, so lifecycle tests
// can establish the ordering between arm and transmit close.
func testktlsConnPair(t *testing.T, install ktlsTxInstaller) (*ktlsTxConn, *tls.Conn) {
	t.Helper()

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
	tassert.CheckFatal(t, err)
	l.install = install

	accepted := make(chan testktlsServer, 1)
	go func() {
		nc, err := l.Accept()
		if err != nil {
			accepted <- testktlsServer{err: err}
			return
		}
		conn, ok := nc.(*ktlsTxConn)
		if !ok {
			_ = nc.Close()
			accepted <- testktlsServer{err: fmt.Errorf("expected *ktlsTxConn, got %T", nc)}
			return
		}
		accepted <- testktlsServer{conn: conn}
	}()

	plain, err := net.Dial("tcp", raw.Addr().String())
	tassert.CheckFatal(t, err)
	client := tls.Client(plain, testktlsClientConf(nil))
	t.Cleanup(func() { _ = client.Close() })

	server := <-accepted
	tassert.CheckFatal(t, server.err)
	// Fake installers can report success without enabling kernel TLS; bypass
	// the kTLS close path during cleanup.
	t.Cleanup(func() { _ = server.conn.tcp.Close() })
	return server.conn, client
}

func testktlsStartHandshake(t *testing.T, server *ktlsTxConn, client *tls.Conn) <-chan error {
	t.Helper()
	ctx := t.Context()
	errCh := make(chan error, 2)
	go func() { errCh <- server.HandshakeContext(ctx) }()
	go func() { errCh <- client.HandshakeContext(ctx) }()
	return errCh
}

func testktlsWaitHandshake(t *testing.T, errCh <-chan error) {
	t.Helper()
	for range 2 {
		select {
		case err := <-errCh:
			tassert.CheckFatal(t, err)
		case <-time.After(testktlsTimeout):
			t.Fatal("timed out waiting for TLS handshake")
		}
	}
}

// ditto, with an explicit template - conf==nil goes through newKTLSTxListener,
// otherwise the listener's template is replaced (to reach states the
// constructor deliberately makes unreachable)
func testktlsHandshakeConf(t *testing.T, conf *tls.Config, install ktlsTxInstaller) *ktlsTxConn {
	t.Helper()

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	l, err := newKTLSTxListener(raw, testktlsServerConf(t), testktlsTimeout, nil)
	tassert.CheckFatal(t, err)
	l.install = install
	if conf != nil {
		l.tlsConfig = conf
	}

	srvCh := testktlsListen(t, l)

	client, err := tls.Dial("tcp", raw.Addr().String(), testktlsClientConf(nil))
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv := <-srvCh
	tassert.CheckFatal(t, srv.err)

	// NOTE: not srv.conn.Close() - once armed, Close takes the kTLS path and
	// would sendmsg an alert on a socket the kernel is not actually driving
	t.Cleanup(func() { _ = srv.conn.tcp.Close() })
	return srv.conn
}
