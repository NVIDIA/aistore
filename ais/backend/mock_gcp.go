//go:build !gcp

// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/stats"
)

func NewGCP(core.TargetPut, stats.Tracker, bool) (core.Backend, error) {
	return nil, &cmn.ErrInitBackend{Provider: apc.GCP}
}
