// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	NetPublic       = "PUBLIC"
	NetIntraControl = "INTRA-CONTROL"
	NetIntraData    = "INTRA-DATA"
)

var KnownNetworks = [...]string{NetPublic, NetIntraControl, NetIntraData}

func NetworkIsKnown(net string) bool {
	return net == NetPublic || net == NetIntraControl || net == NetIntraData
}

func ParsePort(p string) (int, error) {
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}

	return ValidatePort(port)
}

func ValidatePort(port int) (int, error) {
	if port <= 0 || port >= (1<<16) {
		return 0, fmt.Errorf("port number (%d) should be between 1 and 65535", port)
	}
	return port, nil
}

// resolve `host` and returns the first usable (TCP dialable) IP address (v4 or v6)
func Host2IP(host string, localTimeout, preferV6 bool) (net.IP, error) {
	timeout := max(time.Second, Rom.MaxKeepalive())
	if localTimeout {
		timeout = max(time.Second, Rom.CplaneOperation())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	// pass 1: prefer non-loopback, matching preferred address family
	for _, addr := range addrs {
		ip := addr.IP
		if !IsDialableHostIP(ip) || ip.IsLoopback() {
			continue
		}
		isV6 := ip.To4() == nil
		if preferV6 == isV6 {
			return ip, nil
		}
	}

	// pass 2: non-loopback, any family (fallback)
	for _, addr := range addrs {
		ip := addr.IP
		if IsDialableHostIP(ip) && !ip.IsLoopback() {
			return ip, nil
		}
	}

	// pass 3: loopback, preferred family first
	for _, addr := range addrs {
		ip := addr.IP
		if !IsDialableHostIP(ip) {
			continue
		}
		isV6 := ip.To4() == nil
		if preferV6 == isV6 {
			return ip, nil
		}
	}

	// pass 4: any dialable (last resort)
	for _, addr := range addrs {
		ip := addr.IP
		if IsDialableHostIP(ip) {
			return ip, nil
		}
	}

	// return detailed error
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		ips = append(ips, addr.IP)
	}
	return nil, fmt.Errorf("failed to resolve dialable IP for %q (have IPs %v)", host, ips)
}

// check whether `ip` is usable for TCP dialing
// in particular:
// - reject IPv6 link-local
// - reject multicast and unspecified
// - allow loopback
func IsDialableHostIP(ip net.IP) bool {
	if ip == nil {
		return false
	}

	ip4 := ip.To4()
	if ip4 != nil {
		return !ip4.IsUnspecified() && !ip4.IsMulticast()
	}

	ip16 := ip.To16()
	if ip16 == nil {
		return false // invalid
	}

	// filter IPv6
	if ip16.IsUnspecified() || ip16.IsMulticast() {
		return false
	}
	if ip16.IsLinkLocalUnicast() {
		return false // link-local requires a zone
	}
	if ip16[0] == 0x20 && ip16[1] == 0x01 && ip16[2] == 0x0d && ip16[3] == 0xb8 {
		return false // reject RFC 3849 documentation prefix: 2001:db8::/32
	}

	return true
}

func ParseHost2IP(host string, localTimeout, preferV6 bool) (net.IP, error) {
	ip := net.ParseIP(host)
	if ip != nil {
		if !IsDialableHostIP(ip) {
			warn := fmt.Errorf("ParseHost2IP: %s (%s) is not dialable", host, ip.String())
			debug.Assert(false, warn)
			nlog.Warningln(warn)
		}
		return ip, nil // is a parse-able IP addr
	}
	return Host2IP(host, localTimeout, preferV6)
}

func AddrToNetworkFamily(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		debug.Assertf(false, "addr %q is not host:port, err: %v", addr, err)
		return "tcp"
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			return "tcp4"
		}
		return "tcp6"
	}
	return "tcp"
}

//
// concatenate (proto, host, port)
// note: IPv6-safe
//

func HostPort(hostname, port string) string {
	return net.JoinHostPort(hostname, port)
}

// rfc2396.txt "Uniform Resource Identifiers (URI): Generic Syntax"
// (a slightly heavier alternative would be: (&url.URL{Scheme: proto, Host: ep}).String())
func ProtoEndpoint(proto, hostPort string) string {
	return proto + "://" + hostPort
}

func ProtoHostPort(proto, host string, port int) string {
	u := &url.URL{
		Scheme: proto,
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
	}
	return u.String()
}
