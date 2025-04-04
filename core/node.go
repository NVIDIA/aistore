// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	Targets = iota // 0 (core.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
	SelectedNodes
)

type (
	// cluster node
	Node interface {
		SID() string
		String() string
		Snode() *meta.Snode

		Bowner() meta.Bowner
		Sowner() meta.Sowner

		ClusterStarted() bool
		NodeStarted() bool

		StatsUpdater() cos.StatsUpdater

		// Memory allocators
		PageMM() *memsys.MMSA
		ByteMM() *memsys.MMSA
	}
)

func InMaintOrDecomm(smap *meta.Smap, tsi *meta.Snode, xact Xact) error {
	if smap.InMaintOrDecomm(tsi.ID()) {
		return cmn.NewErrXactTgtInMaint(xact.String(), tsi.StringEx())
	}
	return nil
}
