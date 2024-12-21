// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"net"
	"strconv"
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

func Host2IP(host string) (net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("failed to locally resolve %q (have IPs %v)", host, ips)
}

func ParseHost2IP(host string) (net.IP, error) {
	ip := net.ParseIP(host)
	if ip != nil {
		return ip, nil // is a parse-able IP addr
	}
	return Host2IP(host)
}
