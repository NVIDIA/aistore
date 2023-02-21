// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

type StStMap map[string]*stats.NodeStatus // by node ID (SID)

func (ds StStMap) sortedSIDs() (ids []string) {
	ids = make([]string, 0, len(ds))
	for sid := range ds {
		ids = append(ids, sid)
	}
	sort.Strings(ids)
	return
}

func fmtCDF(cdfs map[string]*fs.CDF) string {
	fses := make([]string, 0, len(cdfs))
	for _, cdf := range cdfs {
		fses = append(fses, cdf.FS)
	}
	return fmt.Sprintf("%v", fses)
}
