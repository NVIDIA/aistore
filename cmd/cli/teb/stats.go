// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"sort"

	"github.com/NVIDIA/aistore/stats"
)

type StstMap map[string]*stats.NodeStatus // by node ID (SID)

func (ds StstMap) sortedSIDs() (ids []string) {
	ids = make([]string, 0, len(ds))
	for sid := range ds {
		ids = append(ids, sid)
	}
	sort.Strings(ids)
	return
}
