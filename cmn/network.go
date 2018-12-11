// Package cmn provides common low-level types and utilities for all dfcpub projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strconv"
)

const (
	NetworkPublic       = "public"
	NetworkIntraControl = "intra_control"
	NetworkIntraData    = "intra_data"
)

var KnownNetworks = [3]string{NetworkPublic, NetworkIntraControl, NetworkIntraData}

func NetworkIsKnown(net string) bool {
	return net == NetworkPublic || net == NetworkIntraControl || net == NetworkIntraData
}

func ParsePort(p string) (int, error) {
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}

	if port <= 0 || port >= (1<<16) {
		return 0, fmt.Errorf("port number (%d) should be between 1 and 65535", port)
	}

	return port, nil
}
