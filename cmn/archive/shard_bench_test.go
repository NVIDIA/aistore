// Package archive: write, read, copy, append, list primitives
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/NVIDIA/aistore/cmn/archive"
)

var (
	benchIdx   *archive.ShardIndex
	benchEntry archive.ShardIndexEntry
	benchOK    bool
)

func benchShardIndex(b *testing.B, count int) (*archive.ShardIndex, []byte, []string) {
	b.Helper()
	entries := make(map[string]archive.ShardIndexEntry, count)
	names := make([]string, count)
	for i := range count {
		name := fmt.Sprintf("dataset/shard/member-%08d.jpeg", i)
		names[i] = name
		entries[name] = archive.ShardIndexEntry{Offset: int64(i) * archive.TarBlockSize * 3, Size: 1024}
	}
	idx := &archive.ShardIndex{Entries: entries, SrcSize: 1 << 30}
	packed, err := idx.Pack()
	if err != nil {
		b.Fatal(err)
	}
	return idx, packed, names
}

func BenchmarkShardIndex(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		idx, packed, names := benchShardIndex(b, count)
		b.Run(fmt.Sprintf("pack/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				if _, err := idx.Pack(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("unpack/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				got := &archive.ShardIndex{}
				if err := got.Unpack(packed); err != nil {
					b.Fatal(err)
				}
				benchIdx = got
			}
		})
		b.Run(fmt.Sprintf("read-unpack/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				buf := make([]byte, len(packed))
				if _, err := io.ReadFull(bytes.NewReader(packed), buf); err != nil {
					b.Fatal(err)
				}
				got := &archive.ShardIndex{}
				if err := got.Unpack(buf); err != nil {
					b.Fatal(err)
				}
				benchIdx = got
			}
		})
		b.Run(fmt.Sprintf("lookup-hit/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				benchEntry, benchOK = idx.Entries[names[i%count]]
			}
		})
		b.Run(fmt.Sprintf("lookup-miss/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchEntry, benchOK = idx.Entries["dataset/shard/member-99999999.jpeg"]
			}
		})
	}
}
