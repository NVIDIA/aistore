//go:build !gcp

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/stats"
)

func NewGCP(_ core.TargetPut, _ stats.Tracker) (core.Backend, error) {
	return nil, newErrInitBackend(apc.GCP)
}
