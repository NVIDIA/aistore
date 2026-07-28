// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestExtObjHeader(t *testing.T) {
	expected := testObjHdr()
	body := encodeObjHdr(expected)
	actual, err := ExtObjHeader(body, len(body))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, *expected) {
		t.Fatalf("header mismatch:\nactual:   %+v\nexpected: %+v", actual, expected)
	}
}

func TestExtObjHeaderMalformed(t *testing.T) {
	body := encodeObjHdr(testObjHdr())
	for n := range body {
		_, err := ExtObjHeader(body, n)
		if !errors.Is(err, ErrHdrMalformed) || n > 0 && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated at %d: unexpected error %v", n, err)
		}
	}

	for _, hlen := range []int{-1, len(body) + 1} {
		expected := fmt.Sprintf("%v: declared %d, buffer %d", ErrHdrMalformed, hlen, len(body))
		if _, err := ExtObjHeader(body, hlen); !errors.Is(err, ErrHdrMalformed) || err.Error() != expected {
			t.Fatalf("expected %q, got %v", expected, err)
		}
	}

	body = append(body, 0)
	expected := fmt.Sprintf("%v: decoded %d of %d bytes", ErrHdrLength, len(body)-1, len(body))
	if _, err := ExtObjHeader(body, len(body)); !errors.Is(err, ErrHdrLength) || err.Error() != expected {
		t.Fatalf("expected %q, got %v", expected, err)
	}
}

func TestExtObjHeaderLengthPrefixes(t *testing.T) {
	hdr := &ObjHdr{ObjAttrs: cmn.ObjAttrs{CustomMD: cos.StrKVs{"k": "v"}}}
	body := encodeObjHdr(hdr)
	if len(body) != 48 {
		t.Fatalf("unexpected test header length %d", len(body))
	}

	fields := []struct {
		name string
		off  int
	}{
		{"SID", 0},
		{"bucket name", 4},
		{"provider", 6},
		{"namespace name", 8},
		{"namespace UUID", 10},
		{"object name", 12},
		{"opaque", 14},
		{"demux", 16},
		{"checksum type", 34},
		{"checksum value", 36},
		{"version", 38},
		{"custom key", 40},
		{"custom value", 43},
		{"custom terminator", 46},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			malformed := append([]byte(nil), body...)
			binary.BigEndian.PutUint16(malformed[field.off:], math.MaxUint16)
			expected := fmt.Sprintf("%v: at offset %d: %v", ErrHdrMalformed, field.off+cos.SizeofI16,
				io.ErrUnexpectedEOF)
			if _, err := ExtObjHeader(malformed, len(malformed)); err == nil || err.Error() != expected {
				t.Fatalf("expected %q, got %v", expected, err)
			}
			if _, err := extObjHeader(malformed, len(malformed), hdr); err == nil || err.Error() != expected {
				t.Fatalf("cached: expected %q, got %v", expected, err)
			}
		})
	}
}

func TestExtObjHeaderCache(t *testing.T) {
	firstBody := encodeObjHdr(testObjHdr())
	var prev ObjHdr
	first, err := extObjHeader(firstBody, len(firstBody), &prev)
	if err != nil {
		t.Fatal(err)
	}
	prev = first
	prev.Opaque = nil

	expected := testObjHdr()
	expected.Bck.Name = "next-bucket"
	expected.ObjName = "next-object"
	expected.Opaque = []byte("next-opaque")
	expected.ObjAttrs.Size++
	expected.ObjAttrs.Atime++
	expected.ObjAttrs.SetCksum(cos.ChecksumCesXxh, "fedcba9876543210")
	expected.ObjAttrs.SetCustomKey("key", "next-value")
	body := encodeObjHdr(expected)

	actual, err := extObjHeader(body, len(body), &prev)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, *expected) {
		t.Fatalf("cached decode mismatch:\nactual:   %+v\nexpected: %+v", actual, expected)
	}
}

func TestExtObjHeaderConcurrent(t *testing.T) {
	const (
		streams    = 64
		iterations = 128
	)
	errCh := make(chan error, streams)
	var wg sync.WaitGroup
	for stream := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			headers := [2]*ObjHdr{testObjHdr(), testObjHdr()}
			streamSuffix := fmt.Sprintf("-%d", stream)
			for i, hdr := range headers {
				variantSuffix := fmt.Sprintf("-%d", i)
				hdr.SID += streamSuffix
				hdr.Bck.Name += streamSuffix + variantSuffix
				hdr.Bck.Ns.Name += streamSuffix
				hdr.Demux += streamSuffix
				hdr.ObjName += variantSuffix
				hdr.Opaque = []byte(variantSuffix)
			}
			bodies := [2][]byte{encodeObjHdr(headers[0]), encodeObjHdr(headers[1])}
			var prev ObjHdr
			for i := range iterations {
				idx := i & 1
				actual, err := extObjHeader(bodies[idx], len(bodies[idx]), &prev)
				if err != nil || !reflect.DeepEqual(actual, *headers[idx]) {
					errCh <- fmt.Errorf("stream %d iteration %d: header mismatch: %v", stream, i, err)
					return
				}
				prev = actual
				prev.Opaque = nil
				if _, err := extObjHeader(bodies[idx], 1, &prev); !errors.Is(err, ErrHdrMalformed) {
					errCh <- fmt.Errorf("stream %d iteration %d: unexpected error: %v", stream, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func testObjHdr() *ObjHdr {
	hdr := &ObjHdr{
		Bck: cmn.Bck{
			Name:     "bucket",
			Provider: apc.AIS,
			Ns:       cmn.Ns{Name: "namespace", UUID: "uuid"},
		},
		ObjName: "object",
		SID:     "sender",
		Demux:   "xaction",
		Opaque:  []byte("opaque"),
		ObjAttrs: cmn.ObjAttrs{
			Size:     123,
			Atime:    456,
			Cksum:    cos.NewCksum(cos.ChecksumCesXxh, "0123456789abcdef"),
			CustomMD: cos.StrKVs{"key": "value"},
		},
		Opcode: OpcRequest,
	}
	hdr.ObjAttrs.SetVersion("version")
	return hdr
}

func encodeObjHdr(hdr *ObjHdr) []byte {
	buf := make([]byte, 1024)
	n := insObjHeader(buf, hdr, false)
	return append([]byte(nil), buf[sizeProtoHdr:n]...)
}
