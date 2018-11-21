/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package cmn provides common low-level types and utilities for all dfcpub projects
package cmn

const (
	NetworkPublic       = "public"
	NetworkIntraControl = "intra_control"
	NetworkIntraData    = "intra_data"
)

var KnownNetworks = [3]string{NetworkPublic, NetworkIntraControl, NetworkIntraData}

func NetworkIsKnown(net string) bool {
	return net == NetworkPublic || net == NetworkIntraControl || net == NetworkIntraData
}
