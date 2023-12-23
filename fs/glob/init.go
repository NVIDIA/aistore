// Package glob: this target node (singleton)
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package glob

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/stats"
)

var (
	T      cluster.Target
	Tstats stats.Tracker
)

func Init(t cluster.Target, stats stats.Tracker) {
	T, Tstats = t, stats
}
