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

func NewOCI(core.TargetPut, stats.Tracker, bool) (core.Backend, error) {
	return nil, &cmn.ErrInitBackend{Provider: apc.GCP}
}
