// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestSnodeInitVerifyingKey(t *testing.T) {
	pub, _, err := cos.GenerateNodeSigningKey()
	tassert.CheckFatal(t, err)

	si := &meta.Snode{}
	si.Init("t1234567", apc.Target, pub)

	tassert.Fatalf(t, bytes.Equal(si.VerifyingKey, pub), "verifying key mismatch")
}
