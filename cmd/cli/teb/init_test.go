// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb_test

import (
	"io"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

func TestThrottledPercent(t *testing.T) {
	primary := &meta.Snode{DaeID: "p1"}
	target := &meta.Snode{DaeID: "t1", DaeType: apc.Target}
	smap := &meta.Smap{
		Primary: primary,
		Tmap:    meta.NodeMap{target.ID(): target},
	}
	tmap := teb.NodeStatusMap{
		target.ID(): {
			Status:     teb.NodeOnline,
			Node:       stats.Node{Snode: target},
			MemCPUInfo: apc.MemCPUInfo{CPUThrottled: 37},
		},
	}

	teb.Init(io.Discard, true)
	_, table := teb.MakeTabCPU(smap, nil, tmap)
	if got := table.Template(true); !strings.Contains(got, "37%") || strings.Contains(got, "%!(NOVERB)") {
		t.Fatalf("expected throttled percentage without format artifact, got %q", got)
	}
}
