// Package oci unit test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package oci_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestOCIUserMetadataEncodeDecode(t *testing.T) {
	in := map[string]string{"foo": "bar", "X-Num": "42"}
	hdr := cmn.BackendHelpers.OCI.EncodeMetadata(in)
	if len(hdr) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(hdr))
	}
	if _, ok := hdr["Opc-Meta-Foo"]; !ok {
		t.Fatalf("missing Opc-Meta-Foo, got keys: %v", hdr)
	}
	if _, ok := hdr["Opc-Meta-X-Num"]; !ok {
		t.Fatalf("missing Opc-Meta-X-Num, got keys: %v", hdr)
	}
	h := http.Header{}
	for k, v := range hdr {
		h.Set(k, v)
	}
	out := cmn.BackendHelpers.OCI.DecodeMetadata(h)
	if out["Foo"] != "bar" || out["X-Num"] != "42" {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}
