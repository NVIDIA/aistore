// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import "testing"

func Test1(t *testing.T) {
	uin := uploadInfo{objName: "qrgqersgE4", parts: make(map[int64]*UploadPart)}
	uin.parts[111] = &UploadPart{MD5: "rj*&^!&tj5wh", Size: 111}
	uin.parts[222] = &UploadPart{MD5: "mn^^^n&753rP", Size: 222}

	b := uin.pack()
	uout := &uploadInfo{}
	if err := uout.unpack(b); err != nil {
		t.Fatal(err)
	}
	if uin.objName != uout.objName || len(uin.parts) != len(uout.parts) {
		t.Fatalf("uin != uout: %s, %s, %d, %d", uin.objName, uout.objName, len(uin.parts), len(uout.parts))
	}
	if uin.parts[111].MD5 != uout.parts[111].MD5 {
		t.Fatalf("uin %v != uout %v", *uin.parts[111], *uout.parts[111])
	}
	if uin.parts[222].MD5 != uout.parts[222].MD5 {
		t.Fatalf("uin %v != uout %v", *uin.parts[222], *uout.parts[222])
	}
}
