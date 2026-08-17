// Package env contains environment variables
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package env

//
// environment variables group: integration tests
//

const (
	TestNumTarget = "NUM_TARGET"
	TestNumProxy  = "NUM_PROXY"
	TestNumChunks = "NUM_CHUNKS"
	TestSignHMAC  = "SIGN_HMAC"
	TestRandNs    = "RAND_NS"

	// set this env var for runProviderTests to include erasure-coded-bucket test cases
	// (see ais/test/common for runProviderTests)
	TestRunProviderEC = "INCLUDE_EC_PERMUTATION"
)
