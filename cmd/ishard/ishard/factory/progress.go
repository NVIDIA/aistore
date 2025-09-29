// Package factory provides functions to create shards and track their creation progress
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package factory

import (
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

var p = mpb.New(mpb.WithWidth(64))

func (sf *ShardFactory) NewBar(numShards int64) {
	var (
		text    = "Created Shards Size: "
		total   = numShards
		options = []mpb.BarOption{
			mpb.PrependDecorators(
				decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
				decor.CountersKibiByte("%8.1f / %8.1f"),
			),
			mpb.AppendDecorators(
				decor.NewPercentage("%d", decor.WCSyncSpaceR),
				decor.Elapsed(decor.ET_STYLE_GO, decor.WCSyncWidth),
			),
		}
	)
	sf.pollProgress = p.AddBar(total, options...)
}
