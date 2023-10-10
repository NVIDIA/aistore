// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

// "progress bar" control structures and context
type (
	// negative values indicate that progress information is unavailable
	ProgressInfo struct {
		Percent float64
		Count   int
		Total   int
	}
	ProgressContext struct {
		startTime int64 // time operation started
		callAfter int64 // callback after
		callback  ProgressCallback
		info      ProgressInfo
	}
	ProgressCallback = func(pi *ProgressContext)
)

func NewProgressContext(cb ProgressCallback, after time.Duration) *ProgressContext {
	ctx := &ProgressContext{
		info:      ProgressInfo{Count: -1, Total: -1, Percent: -1.0},
		startTime: mono.NanoTime(),
		callback:  cb,
	}
	ctx.callAfter = ctx.startTime + after.Nanoseconds()
	return ctx
}

func (ctx *ProgressContext) mustFire() bool {
	return ctx.callAfter == ctx.startTime /*immediate*/ ||
		mono.NanoTime() >= ctx.callAfter
}

func (ctx *ProgressContext) finish() {
	ctx.info.Percent = 100.0
	if ctx.info.Total > 0 {
		ctx.info.Count = ctx.info.Total
	}
}

func (ctx *ProgressContext) IsFinished() bool {
	return ctx.info.Percent >= 100.0 ||
		(ctx.info.Total != 0 && ctx.info.Total == ctx.info.Count)
}

func (ctx *ProgressContext) Elapsed() time.Duration {
	return mono.Since(ctx.startTime)
}

func (ctx *ProgressContext) Info() ProgressInfo {
	return ctx.info
}
