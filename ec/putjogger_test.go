// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ec //nolint:testpackage // Tests unexported EC encoding helpers.

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/klauspost/reedsolomon"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestFinalizeSlicesChecksumsPaddedData(t *testing.T) {
	for _, cksumType := range []string{cos.ChecksumCesXxh, cos.ChecksumSHA256} {
		t.Run(cksumType, func(t *testing.T) {
			ctx := newInitializedTestCtx([]byte("abcde"), 2, 1)
			writers, parity := newParityTestWriters(ctx)

			if err := finalizeSlices(ctx, writers, cksumType); err != nil {
				t.Fatal(err)
			}

			assertSliceChecksum(t, ctx.slices[0].cksum, []byte("abc"), cksumType)
			assertSliceChecksum(t, ctx.slices[1].cksum, []byte{'d', 'e', 0}, cksumType)
			assertSliceChecksum(t, ctx.slices[2].cksum, parity[0].Bytes(), cksumType)
		})
	}
}

func TestFinalizeSlicesChecksumsUnpaddedData(t *testing.T) {
	const cksumType = cos.ChecksumCesXxh

	ctx := newInitializedTestCtx([]byte("abcdef"), 2, 1)
	writers, parity := newParityTestWriters(ctx)

	if err := finalizeSlices(ctx, writers, cksumType); err != nil {
		t.Fatal(err)
	}

	assertSliceChecksum(t, ctx.slices[0].cksum, []byte("abc"), cksumType)
	assertSliceChecksum(t, ctx.slices[1].cksum, []byte("def"), cksumType)
	assertSliceChecksum(t, ctx.slices[2].cksum, parity[0].Bytes(), cksumType)
}

func TestFinalizeSlicesChecksumsMultipleParity(t *testing.T) {
	const cksumType = cos.ChecksumCesXxh

	ctx := newInitializedTestCtx([]byte("abcdef"), 2, 2)
	writers, parity := newParityTestWriters(ctx)

	if err := finalizeSlices(ctx, writers, cksumType); err != nil {
		t.Fatal(err)
	}

	assertSliceChecksum(t, ctx.slices[2].cksum, parity[0].Bytes(), cksumType)
	assertSliceChecksum(t, ctx.slices[3].cksum, parity[1].Bytes(), cksumType)
}

func TestInitializeSlices(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want [][]byte
	}{
		{name: "padded", data: []byte("abcde"), want: [][]byte{[]byte("abc"), {'d', 'e', 0}}},
		{name: "unpadded", data: []byte("abcdef"), want: [][]byte{[]byte("abc"), []byte("def")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newInitializedTestCtx(test.data, len(test.want), 1)
			for i, want := range test.want {
				got, err := io.ReadAll(ctx.slices[i].reader)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("slice %d: got %v, expected %v", i, got, want)
				}
			}
		})
	}
}

func TestFinalizeSlicesDoesNotPublishPartialChecksums(t *testing.T) {
	const cksumType = cos.ChecksumCesXxh

	ctx := &encodeCtx{
		dataSlices:   2,
		paritySlices: 1,
		slices:       make([]*slice, 3),
	}
	ctx.slices[0] = &slice{reader: cos.NewByteReader([]byte("abcd"))}
	ctx.slices[1] = &slice{reader: cos.NewByteReader([]byte("abc"))}
	writers, _ := newParityTestWriters(ctx)

	if err := finalizeSlices(ctx, writers, cksumType); err == nil {
		t.Fatal("expected unequal data slice lengths to fail encoding")
	}
	for i, sl := range ctx.slices {
		if sl.cksum != nil {
			t.Fatalf("slice %d published a checksum after failed encoding", i)
		}
	}
}

func TestFinalizeSlicesFailsOnParityWriteError(t *testing.T) {
	const cksumType = cos.ChecksumCesXxh

	errInjected := errors.New("injected parity write error")
	tests := []struct {
		name        string
		failIndices []int
	}{
		{name: "first parity writer", failIndices: []int{0}},
		{name: "second parity writer", failIndices: []int{1}},
		{name: "all parity writers", failIndices: []int{0, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newInitializedTestCtx([]byte("abcdef"), 2, 2)
			writers, _ := newParityTestWriters(ctx)
			for _, i := range test.failIndices {
				writers[i] = errorWriter{err: errInjected}
			}

			err := finalizeSlices(ctx, writers, cksumType)
			var writeErr reedsolomon.StreamWriteError
			if !errors.As(err, &writeErr) {
				t.Fatalf("expected reedsolomon.StreamWriteError, got %v", err)
			}
			if !slices.Contains(test.failIndices, writeErr.Stream) {
				t.Fatalf("error reported for parity writer %d, expected one of %v", writeErr.Stream, test.failIndices)
			}
			if !errors.Is(writeErr.Err, errInjected) {
				t.Fatalf("expected injected write error, got %v", writeErr.Err)
			}
			for i, sl := range ctx.slices {
				if sl.cksum != nil {
					t.Fatalf("slice %d published a checksum after failed encoding", i)
				}
			}
		})
	}
}

// make sure short reads get rejected (see finalizeSlices() for details)
func TestFinalizeSlicesRejectsShortDataSlices(t *testing.T) {
	tests := []struct {
		name       string
		dataSlices int
		data       [][]byte
	}{
		{name: "single data slice", dataSlices: 1, data: [][]byte{[]byte("abcd")}},
		{name: "uniformly short", dataSlices: 2, data: [][]byte{[]byte("abcd"), []byte("efgh")}},
	}
	for _, cksumType := range []string{cos.ChecksumCesXxh, cos.ChecksumNone} {
		t.Run(cksumType, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					ctx := &encodeCtx{
						dataSlices:   test.dataSlices,
						paritySlices: 1,
						sliceSize:    8, // deliberately larger than what the readers hold
						slices:       make([]*slice, test.dataSlices+1),
					}
					for i, data := range test.data {
						ctx.slices[i] = &slice{reader: cos.NewByteReader(data)}
					}
					writers, _ := newParityTestWriters(ctx)

					err := finalizeSlices(ctx, writers, cksumType)
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
					}
					for i, sl := range ctx.slices {
						if sl.cksum != nil {
							t.Fatalf("slice %d published a checksum after a short encode", i)
						}
					}
				})
			}
		})
	}
}

func TestFinalizeSlicesWithoutChecksums(t *testing.T) {
	ctx := newInitializedTestCtx([]byte("abcdef"), 2, 1)
	writers, _ := newParityTestWriters(ctx)

	if err := finalizeSlices(ctx, writers, cos.ChecksumNone); err != nil {
		t.Fatal(err)
	}
	for i, sl := range ctx.slices {
		if sl.cksum != nil {
			t.Fatalf("slice %d unexpectedly has a checksum", i)
		}
	}
}

func newInitializedTestCtx(data []byte, dataSlices, paritySlices int) *encodeCtx {
	ctx := &encodeCtx{
		// this LomHandle carries no LOM - only enough for reading and slicing (and unit testing)
		lh: &core.LomHandle{
			LomReader: cos.NewByteReader(data),
		},
		dataSlices:   dataSlices,
		paritySlices: paritySlices,
	}
	initializeSlices(ctx, int64(len(data)))
	return ctx
}

func newParityTestWriters(ctx *encodeCtx) ([]io.Writer, []*bytes.Buffer) {
	writers := make([]io.Writer, ctx.paritySlices)
	parity := make([]*bytes.Buffer, ctx.paritySlices)
	for i := range ctx.paritySlices {
		parity[i] = &bytes.Buffer{}
		ctx.slices[i+ctx.dataSlices] = &slice{}
		writers[i] = parity[i]
	}
	return writers, parity
}

func assertSliceChecksum(t *testing.T, actual *cos.Cksum, data []byte, cksumType string) {
	t.Helper()
	expected, err := cos.ChecksumBytes(data, cksumType)
	if err != nil {
		t.Fatal(err)
	}
	if !actual.Equal(expected) {
		t.Fatalf("checksum mismatch: got %s, expected %s", actual, expected)
	}
}
