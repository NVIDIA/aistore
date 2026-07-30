// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/core"
)

func TestTargetRenameValidatesObjectNames(t *testing.T) {
	target := &target{}
	for _, test := range []struct {
		source, destination string
	}{
		{"../source", "destination"},
		{"source", "../destination"},
		{"source", "dir/../destination"},
		{"escape-src.txt", "../../../../../../../../tmp/pwned_32114.txt"},
	} {
		err := target.objMv(
			&core.LOM{ObjName: test.source},
			&apc.ActMsg{Name: test.destination},
		)
		if err == nil {
			t.Fatalf("expected rename from %q to %q to fail", test.source, test.destination)
		}
	}
}
