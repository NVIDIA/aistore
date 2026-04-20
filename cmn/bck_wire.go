// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	// Ns (or Namespace) scopes data under the same provider, allowing the
	// same dataset and bucket names to coexist in different namespaces. An
	// empty Ns denotes the default (global) namespace.
	Ns struct {
		// UUID of a remote AIS cluster (used only for the `ais`
		// provider). Multiple namespaces may share the same UUID, so
		// UUID is effectively the parent of the namespace. Leave empty
		// for the global namespace.
		UUID string `json:"uuid" yaml:"uuid" msg:"u"` // +gen:optional
		// Namespace name, unique under a given UUID. Used when building
		// object FQNs. Leave empty for the global namespace.
		Name string `json:"name" yaml:"name" msg:"n"` // +gen:optional
	}

	// Bck uniquely identifies a bucket across providers and namespaces.
	Bck struct {
		Props *Bprops `json:"-" msg:"-"`
		// Bucket name.
		Name string `json:"name" yaml:"name" msg:"n"`
		// Backend provider: one of `"ais"`, `"aws"`, `"gcp"`, `"azure"`,
		// `"oci"`, `"ht"`. Defaults to `"ais"` when empty or omitted.
		// See `api/apc/provider.go` for the supported enum.
		Provider string `json:"provider" yaml:"provider" msg:"p"` // +gen:optional
		// Namespace for scoping the bucket. Leave empty for the default
		// (global) namespace.
		Ns Ns `json:"namespace" yaml:"namespace" list:"omitempty" msg:"s"` // +gen:optional
	}
)
