// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"net/http"
	"net/url"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// go test -bench=Hpool -benchmem -benchtime=10s -run=Hpool

var objs [16384]string

func _args(i int) *cmn.HreqArgs {
	if i >= len(objs) {
		i = 0
	}
	return &cmn.HreqArgs{
		Method: http.MethodGet,
		Base:   "http://www.aistore.com",
		Path:   path.Join(apc.URLPathObjects.S, "bucket", objs[i]),
		Query:  url.Values{"a": []string{"b"}, "provider": []string{"ais"}},
	}
}

func init() {
	for i := range objs {
		objs[i] = path.Join(cos.CryptoRandS(8), cos.CryptoRandS(8))
	}
}

func BenchmarkWitHpool(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			i++
			_, err := _args(i).Req()
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWithoutHpool(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			i++
			_, err := _args(i).ReqDeprecated()
			if err != nil {
				panic(err)
			}
		}
	})
}
