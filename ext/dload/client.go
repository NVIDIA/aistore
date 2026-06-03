// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
)

// SSRF egress guard: the downloader fetches user-supplied URLs, so its client
// must not be usable to reach loopback, link-local (incl. cloud metadata
// 169.254.169.254), or - by policy - private/ULA ranges.

// client.go
var errBlockedEgress = errors.New("dload: egress blocked")

func dloadDialControl(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("dload: refusing to dial non-IP %q", address)
	}
	if dloadBlockedEgress(ip) {
		return fmt.Errorf("%w: %s (%s)", errBlockedEgress, ip, network)
	}
	return nil
}

func dloadBlockedEgress(ip net.IP) bool {
	// never legal
	if ip.IsLoopback() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || // 169.254.0.0/16 (AWS/GCP/Azure IMDS), fe80::/10
		ip.IsMulticast() {
		return true
	}
	// feature-flag configurable: RFC1918 + RFC4193 ULA
	if !cmn.Rom.Features().IsSet(feat.DloadAllowPrivateEgress) && ip.IsPrivate() {
		return true
	}
	return false
}

func newDloadTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   cmn.DfltDialupTimeout,
		KeepAlive: cmn.DfltKeepaliveTCP,
		Control:   dloadDialControl,
	}
	return &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConns:        cmn.DefaultMaxIdleConns,
		MaxIdleConnsPerHost: cmn.DefaultMaxIdleConnsPerHost,
		IdleConnTimeout:     cmn.DefaultIdleConnTimeout,
		WriteBufferSize:     cmn.DefaultWriteBufferSize,
		ReadBufferSize:      cmn.DefaultReadBufferSize,
		DisableCompression:  true,
	}
}

// mirrors cmn.NewDefaultClients, but with the egress-guarded transport
func newDloadClients(timeout time.Duration) (clientH, clientTLS *http.Client) {
	clientH = &http.Client{Transport: newDloadTransport(), Timeout: timeout}

	tr := newDloadTransport()
	tlsConf, err := cmn.NewTLS(cmn.TLSArgs{SkipVerify: true}, false /*intra*/)
	if err != nil {
		cos.ExitLog(err) // FATAL, same as cmn.NewClientTLS
	}
	tr.TLSClientConfig = tlsConf
	clientTLS = &http.Client{Transport: tr, Timeout: timeout}
	return
}
