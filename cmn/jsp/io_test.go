// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package jsp_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/memsys"
)

// go test -v -bench=. -tags=debug
// go test -v -bench=. -tags=debug -benchtime=10s -benchmem

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
		bytes.Equal(ts.B, other.B) &&
		ts.ST.I64 == other.ST.I64 &&
		reflect.DeepEqual(ts.M, other.M)
}

func makeRandStruct() (ts testStruct) {
	if rand.Intn(2) == 0 {
		ts.I = rand.Int()
	}
	ts.S = cos.RandString(rand.Intn(100))
	if rand.Intn(2) == 0 {
		ts.B = []byte(cos.RandString(rand.Intn(200)))
	}
	ts.ST.I64 = rand.Int63()
	if rand.Intn(2) == 0 {
		ts.M = make(map[string]string)
		for i := 0; i < rand.Intn(100)+1; i++ {
			ts.M[cos.RandString(10)] = cos.RandString(20)
		}
	}
	return
}

func makeStaticStruct() (ts testStruct) {
	ts.I = rand.Int()
	ts.S = cos.RandString(100)
	ts.B = []byte(cos.RandString(200))
	ts.ST.I64 = rand.Int63()
	ts.M = make(map[string]string, 10)
	for i := 0; i < 10; i++ {
		ts.M[cos.RandString(10)] = cos.RandString(20)
	}
	return
}

func TestDecodeAndEncode(t *testing.T) {
	tests := []struct {
		name string
		v    testStruct
		opts jsp.Options
	}{
		{name: "empty", v: testStruct{}, opts: jsp.Options{}},
		{name: "default", v: makeRandStruct(), opts: jsp.Options{}},
		{name: "compress", v: makeRandStruct(), opts: jsp.Options{Compress: true}},
		{name: "cksum", v: makeRandStruct(), opts: jsp.Options{Checksum: true}},
		{name: "sign", v: makeRandStruct(), opts: jsp.Options{Signature: true}},
		{name: "compress_cksum", v: makeRandStruct(), opts: jsp.Options{Compress: true, Checksum: true}},
		{name: "cksum_sign", v: makeRandStruct(), opts: jsp.Options{Checksum: true, Signature: true}},
		{name: "ccs", v: makeRandStruct(), opts: jsp.CCSign(1)},
		{
			name: "special_char",
			v:    testStruct{I: 10, S: "abc\ncd]}{", B: []byte{'a', 'b', '\n', 'c', 'd', ']', '}'}},
			opts: jsp.Options{Checksum: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				v    testStruct
				mmsa = memsys.PageMM()
				b    = mmsa.NewSGL(cos.MiB)
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
	mmsa := memsys.PageMM()
	b := mmsa.NewSGL(cos.MiB)
	defer b.Free()

	for i := 0; i < 10000; i++ {
		var (
			x, v string
			opts = jsp.Options{Signature: true, Checksum: true}
		)

		x = cos.RandString(i)

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
		opts jsp.Options
	}{
		{name: "empty", v: testStruct{}, opts: jsp.Options{}},
		{name: "default", v: makeStaticStruct(), opts: jsp.Options{}},
		{name: "sign", v: makeStaticStruct(), opts: jsp.Options{Signature: true}},
		{name: "cksum", v: makeStaticStruct(), opts: jsp.Options{Checksum: true}},
		{name: "compress", v: makeStaticStruct(), opts: jsp.Options{Compress: true}},
		{name: "ccs", v: makeStaticStruct(), opts: jsp.CCSign(7)},
	}
	mmsa := memsys.PageMM()
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			body := mmsa.NewSGL(cos.MiB)
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
		opts jsp.Options
	}{
		{name: "empty", v: testStruct{}, opts: jsp.Options{}},
		{name: "default", v: makeStaticStruct(), opts: jsp.Options{}},
		{name: "sign", v: makeStaticStruct(), opts: jsp.Options{Signature: true}},
		{name: "cksum", v: makeStaticStruct(), opts: jsp.Options{Checksum: true}},
		{name: "compress", v: makeStaticStruct(), opts: jsp.Options{Compress: true}},
		{name: "ccs", v: makeStaticStruct(), opts: jsp.CCSign(13)},
	}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			mmsa, _ := memsys.NewMMSA("jsp")
			defer mmsa.Terminate(false)
			sgl := mmsa.NewSGL(cos.MiB)

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
					r = io.NopCloser(bytes.NewReader(network))
				)
				_, err := jsp.Decode(r, &v, bench.opts, "benchmark")
				tassert.CheckFatal(b, err)
			}
		})
	}
}
