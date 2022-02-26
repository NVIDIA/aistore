// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// object properties (NOTE: embeds system `ObjAttrs` that in turn includes custom user-defined)
type ObjectProps struct {
	ObjAttrs
	Name      string `json:"name"`
	Bck       Bck    `json:"bucket"`
	NumCopies int    `json:"copies"`
	EC        struct {
		Generation   int64 `json:"ec-generation"`
		DataSlices   int   `json:"ec-data"`
		ParitySlices int   `json:"ec-parity"`
		IsECCopy     bool  `json:"ec-replicated"`
	}
	Present bool `json:"present"`
}
