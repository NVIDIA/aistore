// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import "encoding/json"

//go:generate msgp -tests=false -marshal=false

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
