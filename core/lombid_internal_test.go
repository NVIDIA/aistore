// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"testing"

	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLomBid(t *testing.T) {
	tests := []struct {
		bid1, bid2 uint64
		flags      uint16
	}{
		{1, 1 | AisBID, 1},
		{2 | AisBID, 1<<bitshift - 1, 1},
		{1<<bitshift - 1, 2 | AisBID, 1},
		{1234567 | AisBID, 1, 0x7ff},
		{1<<bitshift - 1 | AisBID, 1234567 | AisBID, 0x7f5},
	}

	for _, test := range tests {
		var lid lomBID
		lid = lid.setbid(test.bid1)
		lid = lid.setflags(test.flags)
		tassert.Errorf(t, uint64(lid)&AisBID == test.bid1&AisBID, "expected high bit to match")
		tassert.Errorf(t, lid.flags() == test.flags, "expected %x, got %x", test.flags, lid.flags())
		tassert.Errorf(t, lid.bid() == test.bid1, "expected %x, got %x", test.bid1, lid.bid())

		lid = lid.setbid(test.bid1)
		tassert.Errorf(t, uint64(lid)&AisBID == test.bid1&AisBID, "expected high bit to match")
		tassert.Errorf(t, lid.flags() == test.flags, "expected %x, got %x", test.flags, lid.flags())
		tassert.Errorf(t, lid.bid() == test.bid1, "expected %x, got %x", test.bid1, lid.bid())

		lid = lid.clrflags(test.flags)
		tassert.Errorf(t, uint64(lid) == test.bid1, "expected %x, got %x", test.bid1, lid)
		tassert.Errorf(t, lid.bid() == test.bid1, "expected %x, got %x", test.bid1, lid.bid())

		lid = lid.setbid(test.bid2)
		lid = lid.setflags(test.flags)
		tassert.Errorf(t, uint64(lid)&AisBID == test.bid2&AisBID, "expected high bit to match")
		tassert.Errorf(t, lid.flags() == test.flags, "expected %x, got %x", test.flags, lid.flags())
		tassert.Errorf(t, lid.bid() == test.bid2, "expected %x, got %x", test.bid2, lid.bid())

		lid = lid.clrflags(test.flags)
		tassert.Errorf(t, uint64(lid) == test.bid2, "expected %x, got %x", test.bid2, lid)
		tassert.Errorf(t, lid.bid() == test.bid2, "expected %x, got %x", test.bid2, lid.bid())
	}
}
