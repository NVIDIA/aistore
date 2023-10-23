// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	NetPublic       = "PUBLIC"
	NetIntraControl = "INTRA-CONTROL"
	NetIntraData    = "INTRA-DATA"
)

// NOTE: as of Go 1.16, http.DefaultTransport has the following defaults:
//
//       MaxIdleConns:          100,
//       MaxIdleConnsPerHost :  2 (via DefaultMaxIdleConnsPerHost)
//       IdleConnTimeout:       90 * time.Second,
//       WriteBufferSize:       4KB
//       ReadBufferSize:        4KB
//
// Following are the constants we use by default:

const (
	DefaultMaxIdleConns        = 64
	DefaultMaxIdleConnsPerHost = 16
	DefaultIdleConnTimeout     = 8 * time.Second
	DefaultWriteBufferSize     = 64 * cos.KiB
	DefaultReadBufferSize      = 64 * cos.KiB
	DefaultSendRecvBufferSize  = 128 * cos.KiB
)

var KnownNetworks = []string{NetPublic, NetIntraControl, NetIntraData}

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
