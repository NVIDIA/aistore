// Package env contains environment variables
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package env

// See also:
// - "AIS_READ_HEADER_TIMEOUT"
// - "AIS_DAEMON_ID"
// - "AIS_CLUSTER_CIDR", "AIS_HOST_IP", "AIS_HOST_PORT"
// - "AIS_TARGET_URL"
//
// See also:
// - docs/environment-vars.md

var (
	AIS = struct {
		Endpoint  string
		PrimaryEP string
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
		K8sPod       string
		K8sNode      string
		K8sNamespace string
	}{
		// the way to designate primary when cluster's starting up
		Endpoint:  "AIS_ENDPOINT",
		PrimaryEP: "AIS_PRIMARY_EP",

		// false: HTTP transport, with all the TLS config (below) ignored
		// true:  HTTPS/TLS
		UseHTTPS: "AIS_USE_HTTPS", // cluster config: "net.http.use_https"

		// TLS: client side
		Certificate: "AIS_CRT",
		CertKey:     "AIS_CRT_KEY",
		ClientCA:    "AIS_CLIENT_CA",
		// TLS: common
		SkipVerifyCrt: "AIS_SKIP_VERIFY_CRT", // cluster config: "net.http.skip_verify"

		// variables used in tests and CI
		NumTarget: "NUM_TARGET",
		NumProxy:  "NUM_PROXY",

		// via ais-k8s repo
		// see also:
		// * https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go
		// * docs/environment-vars.md
		K8sPod:       "MY_POD",
		K8sNode:      "MY_NODE",
		K8sNamespace: "K8S_NS",
	}
)
