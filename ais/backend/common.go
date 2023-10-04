// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type ctx = context.Context // used when omitted for shortness sake

var verbose, superVerbose bool

func Init(config *cmn.Config) {
	verbose = config.FastV(4, cos.SmoduleBackend)
	superVerbose = config.FastV(5, cos.SmoduleBackend)
}

func fmtTime(t time.Time) string { return t.Format(time.RFC3339) }

func calcPageSize(pageSize, maxPageSize uint) uint {
	if pageSize == 0 {
		return maxPageSize
	}
	return pageSize
}

//nolint:deadcode,unused // It is used but in `*_mock.go` files.
func newErrInitBackend(provider string) error { return &cmn.ErrInitBackend{Provider: provider} }
