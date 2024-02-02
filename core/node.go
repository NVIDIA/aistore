// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
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
	}
)

func InMaintOrDecomm(smap *meta.Smap, tsi *meta.Snode, xact Xact) (err error) {
	if smap.InMaintOrDecomm(tsi) {
		err = cmn.NewErrXactTgtInMaint(xact.String(), tsi.StringEx())
	}
	return err
}
