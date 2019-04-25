// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

// Shard represents the metadata required to construct a single shard (aka an archive file).
type Shard struct {
	// Size is total size of shard to be created.
	Size int64 `json:"s"`
	// Records contains all metadata to construct the shard.
	Records *Records `json:"r"`
	// Name determines the output name of the shard.
	Name string `json:"n"`
}
