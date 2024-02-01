// Package apc: API control messages and constants
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

type BlobMsg struct {
	ChunkSize  int64
	FullSize   int64
	NumWorkers int
	LatestVer  bool
}
