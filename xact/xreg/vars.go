// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/cmn/atomic"
)

// GenBEID vars (see xact/xreg/uuid)
var (
	PrimeTime atomic.Int64
	MyTime    atomic.Int64
)
