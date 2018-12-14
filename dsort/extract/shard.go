/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

// Shard represents the metadata required to construct a single shard (aka an archive file).
type Shard struct {
	Name   string `json:"n"`
	Bucket string `json:"b"`
	// IsLocal describes if given bucket is local or not.
	IsLocal bool `json:"l"`
	// Size is total size of shard to be created.
	Size int64 `json:"s"`
	// Records contains all metadata to construct the shard.
	Records *Records `json:"r"`
}
