// Package env contains environment variables
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package env

// not included:
// - "AIS_READ_HEADER_TIMEOUT"
// - "AIS_DAEMON_ID"
// - "AIS_HOST_IP", "AIS_HOST_PORT" - local playground (target only)
// - "AIS_TARGET_URL" - ETL
// see also:
// - docs/environment-vars.md

const (
	// endpoint: client | primary startup

	AisEndpoint  = "AIS_ENDPOINT" // the way to designate primary when cluster's starting up
	AisPrimaryEP = "AIS_PRIMARY_EP"

	// networking: two CIDR masks
	// 1. differentiate local (same CIDR) clients for faster HTTP redirect
	// 2. at node startup: when present with multiple choices, select one matching local unicast IP
	//    to use it as node's public interface
	AisLocalRedirectCIDR = "AIS_CLUSTER_CIDR"
	AisPubIPv4CIDR       = "AIS_PUBLIC_IP_CIDR"

	//
	// HTTPS
	// for details and background, see: https://github.com/NVIDIA/aistore/blob/main/docs/environment-vars.md#https
	//

	// false: HTTP transport, with all the TLS config (below) ignored
	// true:  HTTPS/TLS
	// for details and background, see: https://github.com/NVIDIA/aistore/blob/main/docs/environment-vars.md#https
	AisUseHTTPS = "AIS_USE_HTTPS"

	// TLS: client side
	AisClientCert    = "AIS_CRT"
	AisClientCertKey = "AIS_CRT_KEY"
	AisClientCA      = "AIS_CLIENT_CA"

	// client and dev deployment; see also cluster config "net.http.skip_verify"
	AisSkipVerifyCrt = "AIS_SKIP_VERIFY_CRT"

	// via ais-k8s repo
	// see also:
	// * https://github.com/NVIDIA/ais-k8s/blob/main/operator/pkg/resources/cmn/env.go
	// * docs/environment-vars.md
	AisK8sPod                  = "MY_POD"
	AisK8sNode                 = "MY_NODE"
	AisK8sNamespace            = "K8S_NS"
	AisK8sServiceName          = "MY_SERVICE"
	AisK8sPublicHostname       = "AIS_PUBLIC_HOSTNAME"
	AisK8sClusterDomain        = "AIS_K8S_CLUSTER_DOMAIN"
	AisK8sHostNetwork          = "HOST_NETWORK"
	AisK8sEnableExternalAccess = "ENABLE_EXTERNAL_ACCESS"
)
