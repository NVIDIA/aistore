// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ec_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ec"
)

func TestMetadataUnpackValidation(t *testing.T) {
	newValid := func() *ec.Metadata {
		return &ec.Metadata{
			MDVersion:  ec.MDVersionLast,
			Generation: 1,
			Size:       1,
			Data:       cmn.MinSliceCount,
			Parity:     cmn.MinSliceCount,
			Daemons:    cos.MapStrUint16{"target": 0},
		}
	}
	unpack := func(md *ec.Metadata) error {
		return cos.NewUnpacker(md.NewPack()).ReadAny(new(ec.Metadata))
	}

	if err := unpack(newValid()); err != nil {
		t.Fatalf("valid metadata rejected: %v", err)
	}
	maxValid := newValid()
	maxValid.Data = cmn.MaxSliceCount
	maxValid.Parity = cmn.MaxSliceCount
	maxValid.Size = int64(cmn.MaxMonolithicSize)
	maxValid.SliceID = maxValid.Data + maxValid.Parity
	maxValid.Daemons["target"] = uint16(maxValid.SliceID)
	if err := unpack(maxValid); err != nil {
		t.Fatalf("valid boundary metadata rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ec.Metadata)
	}{
		{"generation", func(md *ec.Metadata) { md.Generation = 0 }},
		{"size", func(md *ec.Metadata) { md.Size = -1 }},
		{"oversized", func(md *ec.Metadata) { md.Size = int64(cmn.MaxMonolithicSize) + 1 }},
		{"data", func(md *ec.Metadata) { md.Data = 0 }},
		{"parity", func(md *ec.Metadata) { md.Parity = cmn.MaxSliceCount + 1 }},
		{"slice ID", func(md *ec.Metadata) { md.SliceID = md.Data + md.Parity + 1 }},
		{"daemon slice ID", func(md *ec.Metadata) { md.Daemons["target"] = uint16(md.Data + md.Parity + 1) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			md := newValid()
			test.mutate(md)
			if err := unpack(md); err == nil {
				t.Fatal("expected invalid metadata to be rejected")
			}
		})
	}
}
