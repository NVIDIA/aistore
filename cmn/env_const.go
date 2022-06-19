// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// ais environment variable names (see also fname_const.go)

var (
	EnvVars = struct {
		Endpoint           string
		ShutdownMarkerPath string
		IsPrimary          string
		PrimaryID          string
		SkipVerifyCrt      string
		UseHTTPS           string
		NumTarget          string
		NumProxy           string
		K8sPod             string
	}{
		Endpoint:           "AIS_ENDPOINT",
		IsPrimary:          "AIS_IS_PRIMARY",
		PrimaryID:          "AIS_PRIMARY_ID",
		SkipVerifyCrt:      "AIS_SKIP_VERIFY_CRT",
		UseHTTPS:           "AIS_USE_HTTPS",
		ShutdownMarkerPath: "AIS_SHUTDOWN_MARKER_PATH",

		// Env variables used for tests or CI
		NumTarget: "NUM_TARGET",
		NumProxy:  "NUM_PROXY",

		// via ais-k8s repo (see ais-k8s/operator/pkg/resources/cmn/env.go)
		K8sPod: "MY_POD",
	}
)
