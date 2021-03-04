// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/memsys"
)

type testStruct struct {
	I  int    `json:"a,omitempty"`
	S  string `json:"zero"`
	B  []byte `json:"bytes,omitempty"`
	ST struct {
		I64 int64 `json:"int64"`
	}
	M map[string]string
}

func (ts *testStruct) equal(other testStruct) bool {
	return ts.I == other.I &&
		ts.S == other.S &&
		string(ts.B) == string(other.B) &&
		ts.ST.I64 == other.ST.I64 &&
		reflect.DeepEqual(ts.M, other.M)
}

func makeRandStruct() (ts testStruct) {
	if rand.Intn(2) == 0 {
		ts.I = rand.Int()
	}
	ts.S = cmn.RandString(rand.Intn(100))
	if rand.Intn(2) == 0 {
		ts.B = []byte(cmn.RandString(rand.Intn(200)))
	}
	ts.ST.I64 = rand.Int63()
	if rand.Intn(2) == 0 {
		ts.M = make(map[string]string)
		for i := 0; i < rand.Intn(100)+1; i++ {
			ts.M[cmn.RandString(10)] = cmn.RandString(20)
		}
	}
	return
}

func makeStaticStruct() (ts testStruct) {
	ts.I = rand.Int()
	ts.S = cmn.RandString(100)
	ts.B = []byte(cmn.RandString(200))
	ts.ST.I64 = rand.Int63()
	ts.M = make(map[string]string, 10)
	for i := 0; i < 10; i++ {
		ts.M[cmn.RandString(10)] = cmn.RandString(20)
	}
	return
}

func TestDecodeAndEncode(t *testing.T) {
	tests := []struct {
		name string
		v    testStruct
		opts cmn.Jopts
	}{
		{name: "empty", v: testStruct{}, opts: cmn.Jopts{}},
		{name: "default", v: makeRandStruct(), opts: cmn.Jopts{}},
		{name: "compress", v: makeRandStruct(), opts: cmn.Jopts{Compress: true}},
		{name: "cksum", v: makeRandStruct(), opts: cmn.Jopts{Checksum: true}},
		{name: "sign", v: makeRandStruct(), opts: cmn.Jopts{Signature: true}},
		{name: "compress_cksum", v: makeRandStruct(), opts: cmn.Jopts{Compress: true, Checksum: true}},
		{name: "cksum_sign", v: makeRandStruct(), opts: cmn.Jopts{Checksum: true, Signature: true}},
		{name: "ccs", v: makeRandStruct(), opts: cmn.CCSign(1)},
		{
			name: "special_char",
			v:    testStruct{I: 10, S: "abc\ncd]}{", B: []byte{'a', 'b', '\n', 'c', 'd', ']', '}'}},
			opts: cmn.Jopts{Checksum: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				v testStruct
				b = memsys.DefaultPageMM().NewSGL(cmn.MiB)
			)
			defer b.Free()

			err := jsp.Encode(b, test.v, test.opts)
			tassert.CheckFatal(t, err)

			_, err = jsp.Decode(b, &v, test.opts, "test")
			tassert.CheckFatal(t, err)

			// reflect.DeepEqual may not work here due to using `[]byte` in the struct.
			// `Decode` may generate empty slice from original `nil` slice and while
			// both are kind of the same, DeepEqual says they differ. From output when
			// the test fails:
			//      v(B:[]uint8(nil))   !=   test.v(B:[]uint8{})
			tassert.Fatalf(
				t, v.equal(test.v),
				"structs are not equal, (got: %+v, expected: %+v)", v, test.v,
			)
		})
	}
}

func TestDecodeAndEncodeFuzz(t *testing.T) {
	b := memsys.DefaultPageMM().NewSGL(cmn.MiB)
	defer b.Free()

	for i := 0; i < 10000; i++ {
		var (
			x, v string
			opts = cmn.Jopts{Signature: true, Checksum: true}
		)

		x = cmn.RandString(i)

		err := jsp.Encode(b, x, opts)
		tassert.CheckFatal(t, err)

		_, err = jsp.Decode(b, &v, opts, fmt.Sprintf("%d", i))
		tassert.CheckFatal(t, err)

		tassert.Fatalf(t, x == v, "strings are not equal, (got: %+v, expected: %+v)", x, v)

		b.Reset()
	}
}

func BenchmarkEncode(b *testing.B) {
	benches := []struct {
		name string
		v    testStruct
		opts cmn.Jopts
	}{
		{name: "empty", v: testStruct{}, opts: cmn.Jopts{}},
		{name: "default", v: makeStaticStruct(), opts: cmn.Jopts{}},
		{name: "sign", v: makeStaticStruct(), opts: cmn.Jopts{Signature: true}},
		{name: "cksum", v: makeStaticStruct(), opts: cmn.Jopts{Checksum: true}},
		{name: "compress", v: makeStaticStruct(), opts: cmn.Jopts{Compress: true}},
		{name: "ccs", v: makeStaticStruct(), opts: cmn.CCSign(7)},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			body := memsys.DefaultPageMM().NewSGL(cmn.MiB)
			defer func() {
				b.StopTimer()
				body.Free()
			}()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				err := jsp.Encode(body, bench.v, bench.opts)
				tassert.CheckFatal(b, err)
				body.Reset()
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	benches := []struct {
		name string
		v    testStruct
		opts cmn.Jopts
	}{
		{name: "empty", v: testStruct{}, opts: cmn.Jopts{}},
		{name: "default", v: makeStaticStruct(), opts: cmn.Jopts{}},
		{name: "sign", v: makeStaticStruct(), opts: cmn.Jopts{Signature: true}},
		{name: "cksum", v: makeStaticStruct(), opts: cmn.Jopts{Checksum: true}},
		{name: "compress", v: makeStaticStruct(), opts: cmn.Jopts{Compress: true}},
		{name: "ccs", v: makeStaticStruct(), opts: cmn.CCSign(13)},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			sgl := memsys.DefaultPageMM().NewSGL(cmn.MiB)
			err := jsp.Encode(sgl, bench.v, bench.opts)
			tassert.CheckFatal(b, err)
			network, err := sgl.ReadAll()
			sgl.Free()
			tassert.CheckFatal(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var (
					v testStruct
					r = ioutil.NopCloser(bytes.NewReader(network))
				)
				_, err := jsp.Decode(r, &v, bench.opts, "benchmark")
				tassert.CheckFatal(b, err)
			}
		})
	}
}
