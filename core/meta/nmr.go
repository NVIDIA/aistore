// Package meta: cluster-level metadata
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	NetNamer interface {
		name() string
	}
	netNamerSingle struct {
		parent *Snode
	}
	netNamerMulti struct {
		parent *Snode
		robin  atomic.Uint64 // round
		numIfs uint64        // ## interfaces
	}
)

// interface guard
var _ NetNamer = (*netNamerSingle)(nil)
var _ NetNamer = (*netNamerMulti)(nil)

func (d *Snode) InitNetNamer() {
	l := len(d.PubExtra)
	if l == 0 {
		d.nmr = &netNamerSingle{d}
		return
	}
	d.nmr = &netNamerMulti{parent: d, numIfs: uint64(l + 1)}
}

func (*netNamerSingle) name() string {
	return cmn.NetPublic
}

func (nmr *netNamerMulti) name() string {
	i := nmr.robin.Add(1) % nmr.numIfs
	if i == 0 {
		return cmn.NetPublic
	}
	return nmr.parent.PubExtra[i-1].URL
}
