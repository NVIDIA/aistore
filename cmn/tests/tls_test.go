// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestSetTLSPeerName(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	var (
		srvAddr = srv.Listener.Addr().String()
		srvPEM  = filepath.Join(t.TempDir(), "srv.pem")
		dial    = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, srvAddr)
		}
	)
	_, port, err := net.SplitHostPort(srvAddr)
	tassert.CheckFatal(t, err)
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	tassert.CheckFatal(t, os.WriteFile(srvPEM, b, 0o600))

	// httptest certificate is valid for example.com, *.example.com, and loopback IPs
	tests := []struct {
		name, host, peer string
		external, ok     bool
	}{
		{name: "advertised host outside certificate", host: "node1.internal"},
		{name: "peer_name in certificate", host: "node1.internal", peer: "ais.example.com", ok: true},
		{name: "peer_name not in certificate", host: "node1.internal", peer: "other.org"},
		{name: "advertised host in certificate", host: "node1.example.com", peer: "other.org", ok: true},
		{name: "advertised IP in certificate", host: "127.0.0.1", peer: "other.org", ok: true},
		{name: "advertised IP outside certificate with peer", host: "10.0.0.1", peer: "ais.example.com", ok: true},
		{name: "neither IP nor peer_name in certificate", host: "10.0.0.1", peer: "other.org"},
		{name: "external client ignores peer_name", host: "node1.internal", peer: "ais.example.com", external: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				sargs = cmn.TLSArgs{ClientCA: srvPEM, PeerName: test.peer}
				addr  = net.JoinHostPort(test.host, port)
				tr    = &http.Transport{DialContext: dial}
			)
			tassert.CheckFatal(t, cmn.SetTLS(tr, sargs, !test.external))
			client := &http.Client{Transport: tr}
			defer client.CloseIdleConnections()

			resp, err := client.Get("https://" + addr)
			if resp != nil {
				resp.Body.Close()
			}
			checkPeerTLS(t, "SetTLS", err, test.ok)

			if test.external || test.peer == "" {
				return
			}
			conf, err := cmn.NewTLS(sargs, true /*intra*/)
			tassert.CheckFatal(t, err)
			dialFast := func(string) (net.Conn, error) { return net.Dial("tcp", srvAddr) }
			conn, err := cmn.DialPeerTLS(dialFast, conf, sargs, time.Second)(addr)
			if conn != nil {
				conn.Close()
			}
			checkPeerTLS(t, "DialPeerTLS", err, test.ok)
		})
	}
}

// all failing cases use a trusted certificate, so the only acceptable failure is a name mismatch
func checkPeerTLS(t *testing.T, tag string, err error, ok bool) {
	t.Helper()
	if ok {
		tassert.Errorf(t, err == nil, "%s: unexpected error: %v", tag, err)
		return
	}
	var herr x509.HostnameError
	tassert.Errorf(t, errors.As(err, &herr), "%s: expected hostname error, got %v", tag, err)
}
