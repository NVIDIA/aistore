// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
//msgp:shim cos.BitFlags as:uint64 using:(uint64)/cos.BitFlags
package meta

import (
	"net"

	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	// Snode's networking info
	// swagger:model
	NetInfo struct {
		Hostname    string `json:"node_ip_addr" msg:"h"`
		Port        string `json:"daemon_port" msg:"p"`
		URL         string `json:"direct_url" msg:"u"`
		tcpEndpoint string
	}
	// Snode - a node (gateway or target) in a cluster
	// swagger:model
	Snode struct {
		nmr        NetNamer   `json:"-" msg:"-"`
		LocalNet   *net.IPNet `json:"-" msg:"-"`
		PubNet     NetInfo    `json:"public_net" msg:"p"`        // cmn.NetPublic
		DataNet    NetInfo    `json:"intra_data_net" msg:"d"`    // cmn.NetIntraData
		ControlNet NetInfo    `json:"intra_control_net" msg:"c"` // cmn.NetIntraControl
		DaeType    string     `json:"daemon_type" msg:"t"`       // apc.Proxy | apc.Target
		DaeID      string     `json:"daemon_id" msg:"i"`
		name       string
		PubExtra   []NetInfo    `json:"pub_extra,omitempty" msg:"e,omitempty"`
		Flags      cos.BitFlags `json:"flags" msg:"f"` // enum { SnodeNonElectable, SnodeIC, SnodeMaint, ... }
		IDDigest   uint64       `json:"id_digest" msg:"g"`
	}
)
