// Package fs: internal unit test for fs package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestLoadNodeSigningKeyAcrossMountpaths(t *testing.T) {
	const tid = "t1234567"

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	pub, priv, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)
	pair := cos.NewNodeSigningKey(priv, pub)

	tassert.CheckFatal(t, SetXattr(dir1, xattrNodeSigningKey, pair.Bytes(tid)))
	tassert.CheckFatal(t, SetXattr(dir2, xattrNodeSigningKey, pair.Bytes(tid)))

	got, err := LoadNodeSigningKey(cos.StrKVs{dir1: "", dir2: ""}, tid)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, got.Equal(pair), "loaded key differs")
}

func TestLoadNodeSigningKeyDetectsMismatch(t *testing.T) {
	const tid = "t1234567"

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	pub1, priv1, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)
	pair1 := cos.NewNodeSigningKey(priv1, pub1)

	pub2, priv2, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)
	pair2 := cos.NewNodeSigningKey(priv2, pub2)

	tassert.CheckFatal(t, SetXattr(dir1, xattrNodeSigningKey, pair1.Bytes(tid)))
	tassert.CheckFatal(t, SetXattr(dir2, xattrNodeSigningKey, pair2.Bytes(tid)))

	_, err = LoadNodeSigningKey(cos.StrKVs{dir1: "", dir2: ""}, tid)
	tassert.Fatalf(t, err != nil, "expected signing-key mismatch")
}
