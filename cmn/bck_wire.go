// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	// Ns (or Namespace) adds additional layer for scoping the data under
	// the same provider. It allows to have same dataset and bucket names
	// under different namespaces what allows for easy data manipulation without
	// affecting data in different namespaces.
	Ns struct {
		// UUID of other remote AIS cluster (for now only used for AIS). Note
		// that we can have different namespaces which refer to same UUID (cluster).
		// This means that in a sense UUID is a parent of the actual namespace.
		UUID string `json:"uuid" yaml:"uuid" msg:"u"`
		// Name uniquely identifies a namespace under the same UUID (which may
		// be empty) and is used in building FQN for the objects.
		Name string `json:"name" yaml:"name" msg:"n"`
	}

	Bck struct {
		Props    *Bprops `json:"-" msg:"-"`
		Name     string  `json:"name" yaml:"name" msg:"n"`
		Provider string  `json:"provider" yaml:"provider" msg:"p"` // see api/apc/provider.go for supported enum
		Ns       Ns      `json:"namespace" yaml:"namespace" list:"omitempty" msg:"s"`
	}
)
