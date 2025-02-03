// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

// go test -bench=. -benchmem -benchtime=5s

func BenchmarkWithoutHpool(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			args := &cmn.HreqArgs{
				Method: http.MethodGet,
				Base:   "http://localhost:80",
				Path:   "/v1/bucket",
			}
			_, err := args.ReqDeprecated()
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWitHpool(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			args := &cmn.HreqArgs{
				Method: http.MethodGet,
				Base:   "http://localhost:80",
				Path:   "/v1/bucket",
			}
			_, err := args.Req()
			if err != nil {
				panic(err)
			}
		}
	})
}
