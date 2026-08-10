// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func BenchmarkDloadBlockedEgress(b *testing.B) {
	for name, ip := range map[string]net.IP{
		"public-v4":     net.ParseIP("8.8.8.8"),
		"private-v4":    net.ParseIP("10.0.0.1"),
		"public-v6":     net.ParseIP("2001:4860:4860::8888"),
		"nat64-private": net.ParseIP("64:ff9b::10.0.0.1"),
		"6to4":          net.ParseIP("2002:c001:0203::1"),
		"teredo":        net.ParseIP("2001:0000:4136:e378:8000:63bf:5665:5665"),
	} {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				dloadBlockedEgress(ip)
			}
		})
	}
}

func TestDloadBlocksTransitionEgress(t *testing.T) {
	err := dloadDialControl("tcp6", "[64:ff9b::169.254.169.254]:80", nil)
	if !errors.Is(err, errBlockedEgress) {
		t.Fatalf("expected blocked NAT64 dial, got %v", err)
	}
}

func TestDownloaderVerifiesTLSCertificates(t *testing.T) {
	_, client := newDloadClients(time.Second)
	transport := client.Transport.(*http.Transport)
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("downloader TLS client disables certificate verification")
	}
}
