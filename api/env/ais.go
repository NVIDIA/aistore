// Package env contains environment variables
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package env

var (
	AIS = struct {
		Endpoint  string
		IsPrimary string
		PrimaryID string
		UseHTTPS  string
		// TLS: client side
		Certificate   string
		CertKey       string
		ClientCA      string
		SkipVerifyCrt string
		// tests, CI
		NumTarget string
		NumProxy  string
		// K8s
		K8sPod string
	}{
		Endpoint:  "AIS_ENDPOINT",
		IsPrimary: "AIS_IS_PRIMARY",
		PrimaryID: "AIS_PRIMARY_ID",
		UseHTTPS:  "AIS_USE_HTTPS",

		// TLS: client side
		Certificate:   "AIS_CRT",
		CertKey:       "AIS_CRT_KEY",
		ClientCA:      "AIS_CLIENT_CA",
		SkipVerifyCrt: "AIS_SKIP_VERIFY_CRT",

		// Env variables used for tests or CI
		NumTarget: "NUM_TARGET",
		NumProxy:  "NUM_PROXY",

		// via ais-k8s repo (see ais-k8s/operator/pkg/resources/cmn/env.go)
		K8sPod: "MY_POD",
	}
)
