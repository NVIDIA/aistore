// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn_test

import (
	"net"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestIsBlockedEgressIP(t *testing.T) {
	tests := []struct {
		name         string
		ip           string
		allowPrivate bool
		blocked      bool
	}{
		{"public IPv4", "8.8.8.8", false, false},
		{"public IPv6", "2001:4860:4860::8888", false, false},
		{"loopback", "127.0.0.1", false, true},
		{"IPv4-mapped loopback", "::ffff:127.0.0.1", false, true},
		{"metadata", "169.254.169.254", false, true},
		{"private", "10.0.0.1", false, true},
		{"private allowed", "10.0.0.1", true, false},
		{"ULA", "fd00::1", false, true},
		{"ULA allowed", "fd00::1", true, false},
		{"CGNAT", "100.64.0.1", false, true},
		{"CGNAT with private allowed", "100.64.0.1", true, true},
		{"NAT64 metadata", "64:ff9b::169.254.169.254", false, true},
		{"NAT64 private", "64:ff9b::10.0.0.1", false, true},
		{"NAT64 private allowed", "64:ff9b::10.0.0.1", true, false},
		{"NAT64 public", "64:ff9b::8.8.8.8", false, false},
		{"local NAT64", "64:ff9b:1::8.8.8.8", false, true},
		{"6to4", "2002:c001:0203::1", false, true},
		{"6to4 with private allowed", "2002:c001:0203::1", true, true},
		{"Teredo", "2001:0000:4136:e378:8000:63bf:5665:5665", false, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cmn.IsBlockedEgressIP(net.ParseIP(test.ip), test.allowPrivate); got != test.blocked {
				t.Fatalf("IsBlockedEgressIP(%s, %t) = %t, expected %t", test.ip, test.allowPrivate, got, test.blocked)
			}
		})
	}
	if !cmn.IsBlockedEgressIP(nil, false) {
		t.Fatal("invalid IP must be blocked")
	}
}
