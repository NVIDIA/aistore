// Package apc: API control messages and constants
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type (
	MptCompletedPart struct {
		ETag       string `json:"etag"`
		PartNumber int    `json:"part-number"`
	}
	MptCompletedParts []MptCompletedPart
)

func (m MptCompletedParts) Len() int {
	return len(m)
}
