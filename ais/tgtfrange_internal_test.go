// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRcGrid(t *testing.T) {
	tests := []struct {
		size      int64
		chunkSize int64
		total     int
	}{
		{size: 1, chunkSize: rcChunkSize, total: 1},
		{size: rcChunkSize, chunkSize: rcChunkSize, total: 1},
		{size: rcChunkSize + 1, chunkSize: rcChunkSize, total: 2},
		{size: 10 * cos.GiB, chunkSize: rcChunkSize, total: 1280},
		{size: core.MaxChunkCount * rcChunkSize, chunkSize: rcChunkSize, total: core.MaxChunkCount},
		// beyond core.MaxChunkCount chunks: granularity gets adjusted
		{size: core.MaxChunkCount*rcChunkSize + 1, chunkSize: 2 * rcChunkSize, total: 5000},
		{size: 1 * cos.TiB, chunkSize: 112 * cos.MiB, total: 9363},
	}
	for _, test := range tests {
		chunkSize, total := rcGrid(test.size)
		tassert.Errorf(t, chunkSize == test.chunkSize && total == test.total,
			"size %d: expected (%d, %d), got (%d, %d)", test.size, test.chunkSize, test.total, chunkSize, total)
	}
	// the grid must always fit MaxChunkCount and must always cover the object
	for _, size := range []int64{1, 63, rcChunkSize - 1, 7 * cos.GiB, 977 * cos.GiB, 64 * cos.TiB, 1024 * cos.TiB} {
		chunkSize, total := rcGrid(size)
		tassert.Errorf(t, total > 0 && total <= core.MaxChunkCount, "size %d: invalid chunk count %d", size, total)
		tassert.Errorf(t, int64(total-1)*chunkSize < size && int64(total)*chunkSize >= size,
			"size %d: grid (%d, %d) does not cover the object", size, chunkSize, total)
	}
}

func TestRcID(t *testing.T) {
	var oa cmn.ObjAttrs
	oa.Size = 1024
	tassert.Errorf(t, rcID(&oa) == "", "expected no ID when there's neither version nor ETag")

	oa.SetVersion("1") // remote ais
	id := rcID(&oa)
	tassert.Fatalf(t, cos.ValidateManifestID(id) == nil, "invalid manifest ID %q", id)
	tassert.Errorf(t, rcID(&oa) == id, "ID must be stable")
	oa.SetVersion("2")
	tassert.Errorf(t, rcID(&oa) != id, "ID must change when the remote version changes")

	oa.SetVersion("")
	oa.SetCustomKey(cmn.ETag, "abc")
	id = rcID(&oa)
	tassert.Fatalf(t, cos.ValidateManifestID(id) == nil, "invalid manifest ID %q", id)
	oa.SetCustomKey(cmn.ETag, "xyz")
	tassert.Errorf(t, rcID(&oa) != id, "ID must change when the remote ETag changes")

	id = rcID(&oa)
	oa.Size++
	tassert.Errorf(t, rcID(&oa) != id, "ID must change when the remote size changes")
}

func TestRcPlan(t *testing.T) {
	u, err := core.NewUfest("rcget-0123456789abcdef", nil, false /*must-exist*/)
	tassert.CheckFatal(t, err)

	rc := &rcache{u: u, size: 3*rcChunkSize + 100, chunkSize: rcChunkSize, total: 4}
	ranges := []htrange{
		{Start: 0, Length: 1},                                 // first chunk
		{Start: 3 * rcChunkSize, Length: 100},                 // last (partial) chunk
		{Start: rcChunkSize - 10, Length: 20},                 // across a chunk boundary
		{Start: rcChunkSize - 10, Length: 2*rcChunkSize + 20}, // unaligned, 4 chunks
		{Start: 0, Length: rc.size},                           // entire object
	}
	// nothing is cached: expecting a single (coalesced) segment in either case
	for _, filling := range []bool{false, true} {
		rc.filling = filling
		for _, hrng := range ranges {
			segs := rc.plan(&hrng)
			tassert.Fatalf(t, len(segs) == 1, "filling=%t, %+v: expected 1 segment, got %d", filling, hrng, len(segs))

			s := segs[0]
			tassert.Errorf(t, s.off == hrng.Start && s.size == hrng.Length,
				"filling=%t, %+v: unexpected sub-range %+v", filling, hrng, s)
			if !filling {
				tassert.Errorf(t, s.count == 0, "filling=%t, %+v: not expecting to store %+v", filling, hrng, s)
				continue
			}
			// the run must be grid-aligned and must cover the requested sub-range
			last := int((hrng.Start + hrng.Length - 1) / rcChunkSize)
			tassert.Errorf(t, s.num == int(hrng.Start/rcChunkSize)+1 && s.count == last-s.num+2,
				"%+v: unexpected chunk run %+v", hrng, s)
			tassert.Errorf(t, s.coff == int64(s.num-1)*rcChunkSize && s.coff+s.clen == min(rc.size, int64(last+1)*rcChunkSize),
				"%+v: unexpected run extent %+v", hrng, s)
			tassert.Errorf(t, s.off >= s.coff && s.off+s.size <= s.coff+s.clen,
				"%+v: sub-range not within the run %+v", hrng, s)
		}
	}
}

func TestSubrangeWriter(t *testing.T) {
	const l = 100
	src := make([]byte, l)
	for i := range src {
		src[i] = byte(i)
	}
	tests := []struct{ skip, size int64 }{
		{skip: 0, size: l},   // all of it
		{skip: 0, size: 10},  // head
		{skip: 90, size: 10}, // tail
		{skip: 30, size: 50}, // middle
		{skip: 99, size: 1},  // single byte
		{skip: l, size: 0},   // nothing
	}
	for _, test := range tests {
		for _, piece := range []int{1, 7, l} { // write in variable-size pieces
			var (
				w  bytes.Buffer
				sw = &subrangeWriter{w: &w, skip: test.skip, left: test.size}
			)
			for off := 0; off < l; off += piece {
				end := min(off+piece, l)
				n, err := sw.Write(src[off:end])
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, n == end-off, "%+v: short write %d (expecting %d)", test, n, end-off)
			}
			tassert.Errorf(t, sw.n == test.size, "%+v: forwarded %d bytes", test, sw.n)
			tassert.Errorf(t, bytes.Equal(w.Bytes(), src[test.skip:test.skip+test.size]),
				"%+v: wrong sub-range (piece size %d)", test, piece)
		}
	}
}
