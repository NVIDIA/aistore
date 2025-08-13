// Package apc: API control messages and constants
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "strings"

type ETLPipeline []string

const ETLPipelineSeparator = ","

func (ep *ETLPipeline) Join(node string) {
	*ep = append(*ep, node)
}

func (ep *ETLPipeline) Pack() string {
	return strings.Join(*ep, ETLPipelineSeparator)
}

func (ep *ETLPipeline) String() string {
	return ep.Pack()
}
