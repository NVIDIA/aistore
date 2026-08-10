//go:build dsort

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import "testing"

func TestEKMBlocks6to4Egress(t *testing.T) {
	if err := ekmDialControl("tcp6", "[2002:c001:0203::1]:80", nil); err == nil {
		t.Fatal("expected blocked 6to4 dial")
	}
}
