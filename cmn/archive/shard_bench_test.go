// Package archive: write, read, copy, append, list primitives
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	benchBytes []byte
	benchEntry archive.ShardIndexEntry
	benchOK    bool
)

func benchShardIndex(b *testing.B, count int) (*archive.ShardIndex, []byte, []string, map[string]archive.ShardIndexEntry) {
	b.Helper()
	entries := make(map[string]archive.ShardIndexEntry, count)
	names := make([]string, count)
	for i := range count {
		name := fmt.Sprintf("dataset/shard/member-%08d.jpeg", i)
		names[i] = name
		entries[name] = archive.ShardIndexEntry{Offset: int64(i) * archive.TarBlockSize * 3, Size: 1024}
	}
	idx, err := archive.NewShardIndexTestOnly(nil, 1<<30, entries)
	if err != nil {
		b.Fatal(err)
	}
	packed, err := idx.Pack()
	if err != nil {
		b.Fatal(err)
	}
	return idx, packed, names, entries
}

func BenchmarkShardIndex(b *testing.B) {
	mm := memsys.PageMM()
	for _, count := range []int{1_000, 10_000, 100_000} {
		idx, packed, names, entries := benchShardIndex(b, count)
		b.Run(fmt.Sprintf("new-pack/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				got, err := archive.NewShardIndexTestOnly(nil, 1<<30, entries)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := got.Pack(); err != nil {
					b.Fatal(err)
				}
				got.Free()
			}
		})
		b.Run(fmt.Sprintf("pack/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				benchBytes, _ = idx.Pack()
			}
		})
		b.Run(fmt.Sprintf("read-unpack-heap/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				got, err := archive.ReadShardIndex(bytes.NewReader(packed), int64(len(packed)), nil)
				if err != nil {
					b.Fatal(err)
				}
				got.Free()
			}
		})
		b.Run(fmt.Sprintf("read-unpack-mmsa/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for b.Loop() {
				got, err := archive.ReadShardIndex(bytes.NewReader(packed), int64(len(packed)), mm)
				if err != nil {
					b.Fatal(err)
				}
				got.Free()
			}
		})
		b.Run(fmt.Sprintf("lookup-hit/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				benchEntry, benchOK = idx.Lookup(names[i%count])
			}
		})
		b.Run(fmt.Sprintf("lookup-miss/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchEntry, benchOK = idx.Lookup("dataset/shard/member-99999999.jpeg")
			}
		})
		idx.Free()
	}
}
