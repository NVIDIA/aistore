// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"testing"

	"github.com/NVIDIA/aistore/tools/trand"
)

func TestPackUnpack(t *testing.T) {
	const nump = 10
	var (
		in  = mpt{parts: make([]*MptPart, 0)}
		out = &mpt{}
	)
	for i := int64(0); i < nump; i++ {
		in.parts = append(in.parts, &MptPart{Num: 111 + i*i, MD5: trand.String(8), Size: 1024 + i})
	}
	b := in.pack()
	if err := out.unpack(b); err != nil {
		t.Fatal(err)
	}
	if len(in.parts) != len(out.parts) {
		t.Fatalf("in != out: %d, %d", len(in.parts), len(out.parts))
	}
	for i := 0; i < nump; i++ {
		if *in.parts[i] != *out.parts[i] {
			t.Fatalf("in %v != out %v", *in.parts[i], *out.parts[i])
		}
	}
}
