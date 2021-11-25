// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	OutObjCount = "streams.out.obj.n"
	OutObjSize  = "streams.out.obj.size"
	InObjCount  = "streams.in.obj.n"
	InObjSize   = "streams.in.obj.size"
)

type (
	// stream stats
	Stats struct {
		Num            atomic.Int64 // number of transferred objects including zero size (header-only) objects
		Size           atomic.Int64 // transferred object size (does not include transport headers)
		Offset         atomic.Int64 // stream offset, in bytes
		CompressedSize atomic.Int64 // compressed size (NOTE: converges to the actual compressed size over time)
	}
)

var statsTracker cos.StatsTracker
