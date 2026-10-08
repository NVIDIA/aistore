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

	"github.com/tinylib/msgp/msgp"
)

func TestSnodeInitVerifyingKey(t *testing.T) {
	pub, _, err := cos.GenerateNodeKeyPair()
	tassert.CheckFatal(t, err)

	si := &meta.Snode{}
	si.Init("t1234567", apc.Target, pub)

	tassert.Fatalf(t, cos.CryptoEqual(si.VerifyingKey, pub), "verifying key mismatch")
}

func TestSnodePlacementWeightWire(t *testing.T) {
	for _, weight := range []int64{0, 30, 600 << 30, 1<<63 - 1} {
		si := &meta.Snode{}
		si.Init("t1234567", apc.Target, nil)
		si.SetPlacementWeight(weight)
		var buf bytes.Buffer
		writer := msgp.NewWriter(&buf)
		tassert.CheckFatal(t, si.EncodeMsg(writer))
		tassert.CheckFatal(t, writer.Flush())
		tassert.Fatalf(t, buf.Len() <= si.Msgsize(), "encoded size exceeds Msgsize")
		var decoded meta.Snode
		tassert.CheckFatal(t, decoded.DecodeMsg(msgp.NewReader(bytes.NewReader(buf.Bytes()))))
		tassert.Fatalf(t, decoded.PlacementWeight() == weight, "weight %d became %d", weight, decoded.PlacementWeight())

		reader := msgp.NewReader(bytes.NewReader(buf.Bytes()))
		n, err := reader.ReadMapHeader()
		tassert.CheckFatal(t, err)
		var hasWeight bool
		for range n {
			key, err := reader.ReadString()
			tassert.CheckFatal(t, err)
			hasWeight = hasWeight || key == "pl"
			tassert.CheckFatal(t, reader.Skip())
		}
		tassert.Fatalf(t, hasWeight == (weight != 0), "weight field omission mismatch for %d", weight)
	}
}
