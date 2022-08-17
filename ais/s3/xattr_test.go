// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import "testing"

func Test1(t *testing.T) {
	uin := mpt{objName: "qrgqersgE4", parts: make([]*MptPart, 2)}
	uin.parts[0] = &MptPart{Num: 111, MD5: "rj*&^!&tj5wh", Size: 1024}
	uin.parts[1] = &MptPart{Num: 222, MD5: "mn^^^n&753rP", Size: 2048}

	b := uin.pack()
	uout := &mpt{}
	if err := uout.unpack(b); err != nil {
		t.Fatal(err)
	}
	if uin.objName != uout.objName || len(uin.parts) != len(uout.parts) {
		t.Fatalf("uin != uout: %s, %s, %d, %d", uin.objName, uout.objName, len(uin.parts), len(uout.parts))
	}
	if *uin.parts[0] != *uout.parts[0] {
		t.Fatalf("uin %v != uout %v", *uin.parts[0], *uout.parts[0])
	}
	if *uin.parts[1] != *uout.parts[1] {
		t.Fatalf("uin %v != uout %v", *uin.parts[1], *uout.parts[1])
	}
}
