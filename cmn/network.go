// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

func Host2IP(host string, local bool) (net.IP, error) {
	timeout := max(time.Second, Rom.MaxKeepalive())
	if local {
		timeout = max(time.Second, Rom.CplaneOperation())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ip := addr.IP.To4(); ip != nil {
			return ip, nil
		}
	}

	// return err
	ips := make([]net.IP, len(addrs))
	for i, addr := range addrs {
		ips[i] = addr.IP
	}
	return nil, fmt.Errorf("failed to locally resolve %q (have IPs %v)", host, ips)
}

func ParseHost2IP(host string, local bool) (net.IP, error) {
	ip := net.ParseIP(host)
	if ip != nil {
		return ip, nil // is a parse-able IP addr
	}
	return Host2IP(host, local)
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
