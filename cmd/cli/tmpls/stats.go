// Package tmpls provides the set of templates used to format output for the CLI.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package tmpls

import (
	"github.com/NVIDIA/aistore/stats"
)

// colDisk = "DISK"
// colCapUsed   = "CAP USED(%)"
// colCapAvail  = "CAP AVAIL"

// TODO -- FIXME -- WIP -- ds *stats.DaemonStats and stats.ClusterStats
func NewTargetStats(*stats.DaemonStats) *Table {
	return nil
}
