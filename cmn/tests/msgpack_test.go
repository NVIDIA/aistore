// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/memsys"
	"github.com/tinylib/msgp/msgp"
)

//
// NOTE: run `msgp -file msgpack_test.go -tests=false -unexported` to generate the code and see docs/msgp.md for details.
//

const (
	bzero = "\x00"
	bone  = "\x01"
	btwo  = "\xe3/\xbd"
)

type shard map[string][]byte

func TestMspGeneric(t *testing.T) {
	num := 1000
	in := make(shard, num)

	// 1. fill
	for i := 1; i < num; i++ {
		k := strconv.Itoa(i * 94646581247)
		// append non-ascii into the key and then to the content as well
		switch i % 3 {
		case 0:
			k = bzero + k + bone
		case 1:
			k = bone + k + btwo
		case 2:
			k = btwo + bzero + bzero + k // + bzero // NOTE: no binary zeros at the string's end
		}
		in[k] = []byte(strings.Repeat(k, i))
	}

	// 2. encode map[string][]byte => sgl
	var (
		sgl = memsys.PageMM().NewSGL(0)
		mw  = msgp.NewWriter(sgl)
	)
	err := in.EncodeMsg(mw)
	if err != nil {
		t.Fatal(err)
	}

	// 3. unmarshal from sgl
	var out shard
	_, err = out.UnmarshalMsg(sgl.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	// 4. double-check
	if len(in) != len(out) {
		t.Fatalf("not ok: %d != %d", len(in), len(out))
	}
	for k, vin := range in {
		vout, ok := out[k]
		if !ok {
			t.Fatalf("%s does not exist", k)
		}
		if !bytes.Equal(vin, vout) {
			t.Fatalf("%s not equal", k)
		}
	}
}
