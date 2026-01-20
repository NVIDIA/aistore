//go:build !sharding

// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import "github.com/NVIDIA/aistore/core/meta"

func (*Trunner) regDsort(*meta.Snode) {}
