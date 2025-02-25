// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import "encoding/json"

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
//

// interface guard
var (
	_ json.Marshaler   = (*Shard)(nil)
	_ json.Unmarshaler = (*Shard)(nil)
)

type (
	// Shard represents the metadata required to construct a single shard (aka an archive file).
	Shard struct {
		// Size is total size of shard to be created.
		Size int64 `msg:"s"`
		// Records contains all metadata to construct the shard.
		Records *Records `msg:"r"`
		// Name determines the output name of the shard.
		Name string `msg:"n"`
	}
)

func (*Shard) MarshalJSON() ([]byte, error) { panic("should not be used") }
func (*Shard) UnmarshalJSON([]byte) error   { panic("should not be used") }
