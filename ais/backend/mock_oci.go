//go:build !oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/stats"
)

func NewOCI(_ core.TargetPut, _ stats.Tracker) (core.Backend, error) {
	_, e := byteSizeParse("")
	if e != nil {
		return nil, e
	}
	return nil, &cmn.ErrInitBackend{Provider: apc.GCP}
}
