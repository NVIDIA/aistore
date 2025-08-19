// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLomBid(t *testing.T) {
	tests := []struct {
		bid1, bid2 uint64
		flags      lomFlags
	}{
		{1, 2, lmflFntl},
		{1234567, 9876543, lmflChunk},
		{1<<59 - 1, 42, lmflFntl | lmflChunk}, // max bucket ID
	}

	for _, test := range tests {
		var lid lomBID

		lid = lid.setbid(test.bid1)
		lid = lid.setlmfl(test.flags)

		tassert.Errorf(t, test.bid1 == lid.bid(), "expected bid %x, got %x", test.bid1, lid.bid())
		tassert.Errorf(t, test.flags == lid.flags(), "expected flags %x, got %x", test.flags, lid.flags())
		tassert.Errorf(t, lid.haslmfl(test.flags), "expected flags %x to be set", test.flags)

		lid = lid.clrlmfl(test.flags)
		tassert.Errorf(t, test.bid1 == lid.bid(), "bid should remain %x after clearing flags", test.bid1)
		tassert.Errorf(t, lomFlags(0) == lid.flags(), "flags should be cleared")

		lid = lid.setbid(test.bid2)
		lid = lid.setlmfl(test.flags)

		tassert.Errorf(t, test.bid2 == lid.bid(), "expected bid %x, got %x", test.bid2, lid.bid())
		tassert.Errorf(t, test.flags == lid.flags(), "expected flags %x, got %x", test.flags, lid.flags())
	}
}
