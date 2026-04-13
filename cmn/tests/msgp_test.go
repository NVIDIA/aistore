// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"

	"github.com/tinylib/msgp/msgp"
)

func TestNodeStatusMsgpSmoke(t *testing.T) {
	orig := stats.NodeStatus{}
	var buf bytes.Buffer
	w := msgp.NewWriter(&buf)
	if err := orig.EncodeMsg(w); err != nil {
		t.Fatal(err)
	}

	err := w.Flush()
	debug.AssertNoErr(err)

	var decoded stats.NodeStatus
	r := msgp.NewReader(&buf)
	if err := decoded.DecodeMsg(r); err != nil {
		t.Fatal(err)
	}
}
