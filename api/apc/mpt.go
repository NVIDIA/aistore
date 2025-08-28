// Package apc: API control messages and constants
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type (
	MptCompletedPart struct {
		PartNumber int    `json:"part-number"`
		ETag       string `json:"etag"`
	}
	MptCompletedParts []MptCompletedPart
)

func (m MptCompletedParts) Len() int {
	return len(m)
}
