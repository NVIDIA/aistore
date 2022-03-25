// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"bytes"
	"fmt"
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

func TestMspGenericShardUnmarshal(t *testing.T) {
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
		fmt.Println(t.Name(), err)
		return
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

func TestMspGenericShardDecode(t *testing.T) {
	num := 2000
	in := make(shard, num)

	// 1. fill
	for i := 1; i < num; i++ {
		var pattern, k string
		k = strconv.Itoa(i * 94646581247)
		// append non-ascii into the key and then to the content as well
		switch i % 3 {
		case 0:
			pattern = k + bone
		case 1:
			pattern = bone + k + btwo
		case 2:
			pattern = btwo + bzero + bzero + bzero
		}
		in[k] = []byte(strings.Repeat(pattern, i))
	}

	// 2. encode map[string][]byte => sgl
	var (
		sgl = memsys.PageMM().NewSGL(memsys.MaxPageSlabSize, memsys.MaxPageSlabSize)
		mw  = msgp.NewWriter(sgl)
	)
	err := in.EncodeMsg(mw)
	if err != nil {
		t.Fatal(err)
	}

	// 3. unmarshal from sgl
	var (
		out    shard
		buf, _ = memsys.PageMM().AllocSize(memsys.MaxPageSlabSize)
	)
	err = out.DecodeMsg(msgp.NewReaderBuf(sgl, buf))
	if err != nil {
		fmt.Println(t.Name(), err)
		return
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
