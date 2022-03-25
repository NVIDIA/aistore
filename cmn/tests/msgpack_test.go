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

	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/vmihailenco/msgpack/v5"
)

//
// NOTE: github.com/vmihailenco/msgpack/v5 (here) vs github.com/tinylib/msgp/msgp
//       - tinylib reqires code-gen
//       - tinylib fails these tests in about 5% to 10% of the time
//

const (
	bzero = "\x00"
	bone  = "\x01"
	btwo  = "\xe3/\xbd"
)

// generic `shard`
type shard map[string][]byte

func TestMsgpackGenericShardMarshal(t *testing.T) {
	for i := 0; i < 3; i++ {
		var (
			dst      interface{}
			in       = makeShard(1000 /* num files in a shard */, false /* non-ascii key*/)
			buf, err = msgpack.Marshal(in)
		)
		if err != nil {
			t.Fatal(err)
		}
		// see python/README.md
		// ioutil.WriteFile("/tmp/packed/shard."+strconv.Itoa(i), buf, cos.PermRWR)

		// unmarshal
		err = msgpack.Unmarshal(buf, &dst)
		if err != nil {
			t.Fatal(err)
		}
		// perform checks
		cmpShard(t, in, dst)
	}
}

func TestMsgpackGenericShardEncode(t *testing.T) {
	for i := 0; i < 3; i++ {
		var (
			dst interface{}
			in  = makeShard(1000 /* num files in a shard */, false /* non-ascii key*/)
			sgl = memsys.PageMM().NewSGL(0)
			enc = msgpack.NewEncoder(sgl)
			err = enc.Encode(in)
		)
		if err != nil {
			t.Fatal(err)
		}

		// decode from sgl; NOTE: uses sgl as both io.Reader and io.ByteScanner
		dec := msgpack.NewDecoder(sgl)
		err = dec.Decode(&dst)
		if err != nil {
			t.Fatal(err)
		}
		// perform checks
		cmpShard(t, in, dst)
	}
}

func makeShard(num int, nonASCIIKey bool) (s shard) {
	s = make(shard, num)
	now := mono.NanoTime()
	for i := 1; i < num; i++ {
		k := strconv.FormatUint(uint64(i)*uint64(now), 16)
		pattern := k
		// append non-ascii into the key and then to the content as well
		switch i % 3 {
		case 0:
			pattern = bzero + k + bone
		case 1:
			pattern = bone + k + btwo
		case 2:
			pattern = btwo + bzero + bzero + k + bzero
		}
		if nonASCIIKey {
			k = pattern
		}
		s[k] = []byte(strings.Repeat(pattern, i))
	}
	return
}

func cmpShard(t *testing.T, in shard, dst interface{}) {
	out, ok := dst.(map[string]interface{})
	if !ok {
		t.Fatalf("not ok: %T", dst)
	}
	if len(in) != len(out) {
		t.Fatalf("not ok: %d != %d", len(in), len(out))
	}
	for k, vin := range in {
		v, ok := out[k]
		if !ok {
			t.Fatalf("%s does not exist", k)
		}
		vout := v.([]byte)
		if !bytes.Equal(vin, vout) {
			t.Fatalf("%s not equal", k)
		}
	}
}
