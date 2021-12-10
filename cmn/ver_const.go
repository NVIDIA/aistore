// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/jsp"

// ========================== IMPORTANT NOTE ==============================
//
// - (major.minor) version indicates the current version of AIS software
//   and is updated manually prior to each release;
//   making a build with an updated version is the precondition to
//   creating the corresponding git tag
//
// - MetaVer* constants, on the other hand, specify all current on-disk
//   formatting versions (meta-versions) with the intent to support backward
//   compatibility in the future
//
// - Most of the enumerated types below utilize `jsp` package to serialize
//   and format their (versioned) instances. In its turn, `jsp` itself
//   has a certain meta-version that corresponds to the specific way
//   `jsp` formats its *signature* and other implementation details.

const AIStoreSoftwareVersion = "3.8"

const (
	MetaverSmap  = 1 // Smap (cluster map) formatting version (jsp)
	MetaverBMD   = 1 // BMD (bucket metadata) --/-- (jsp)
	MetaverRMD   = 1 // Rebalance MD (jsp)
	MetaverVMD   = 1 // Volume MD (jsp)
	MetaverEtlMD = 1 // ETL MD (jsp)

	MetaverLOM = 1 // LOM

	MetaverConfig      = 1 // Global Configuration (jsp)
	MetaverAuthNConfig = 1 // Authn config (jsp) // ditto
	MetaverAuthTokens  = 1 // Authn tokens (jsp) // ditto

	MetaverMetasync = 1 // metasync over network formatting version (jsp)

	MetaverJSP = jsp.Metaver // `jsp` own encoding version
)
