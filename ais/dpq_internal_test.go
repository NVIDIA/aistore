// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestDpqETLTransformArgs(t *testing.T) {
	for _, args := range []string{
		`{"value":"a+b%20&c"}`,
		`hello world`,
		`a/b?c=d&e=f`,
	} {
		q := url.Values{}
		q.Set(apc.QparamETLTransformArgs, args)

		dpq := dpqAlloc()
		err := dpq.parse(q.Encode())
		tassert.CheckFatal(t, err)

		actual := dpq.get(apc.QparamETLTransformArgs)
		tassert.Errorf(t, actual == args, "expected %q, got %q", args, actual)
		dpqFree(dpq)
	}
}
