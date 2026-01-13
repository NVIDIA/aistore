// Package env contains environment variables
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package env

// AuthN environment names
// see also: docs/environment-vars.md

//nolint:gosec // false positive G101
const (
	AisAuthEnabled        = "AIS_AUTHN_ENABLED"
	AisAuthURL            = "AIS_AUTHN_URL"
	AisAuthTokenFile      = "AIS_AUTHN_TOKEN_FILE" // fully qualified
	AisAuthToken          = "AIS_AUTHN_TOKEN"      // Only the JWT token itself (excluding the file and JSON)
	AisAuthConfDir        = "AIS_AUTHN_CONF_DIR"   // contains AuthN config and tokens DB
	AisAuthLogDir         = "AIS_AUTHN_LOG_DIR"
	AisAuthLogLevel       = "AIS_AUTHN_LOG_LEVEL"
	AisAuthExternalURL    = "AIS_AUTHN_EXTERNAL_URL"
	AisAuthPort           = "AIS_AUTHN_PORT"
	AisAuthTTL            = "AIS_AUTHN_TTL"
	AisAuthUseHTTPS       = "AIS_AUTHN_USE_HTTPS"
	AisAuthServerCrt      = "AIS_SERVER_CRT"
	AisAuthServerKey      = "AIS_SERVER_KEY"
	AisAuthSecretKey      = "AIS_AUTHN_SECRET_KEY"
	AisAuthPrivateKeyFile = "AIS_AUTHN_PRIVATE_KEY_FILE"
	AisAuthPublicKey      = "AIS_AUTHN_PUBLIC_KEY" // for asymmetric tokens
	AisAuthAdminUsername  = "AIS_AUTHN_SU_NAME"
	AisAuthAdminPassword  = "AIS_AUTHN_SU_PASS"
)
