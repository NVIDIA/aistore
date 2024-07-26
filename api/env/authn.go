// Package env contains environment variables
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package env

// authn environment variables
// see also: docs/environment-vars.md

var (
	AuthN = struct {
		Enabled       string
		URL           string
		TokenFile     string
		ConfDir       string
		LogDir        string
		LogLevel      string
		Port          string
		TTL           string
		UseHTTPS      string
		ServerCrt     string
		ServerKey     string
		AdminPassword string
		AdminUsername string
		SecretKey     string
	}{
		Enabled:       "AIS_AUTHN_ENABLED",
		URL:           "AIS_AUTHN_URL",
		TokenFile:     "AIS_AUTHN_TOKEN_FILE", // fully qualified
		ConfDir:       "AIS_AUTHN_CONF_DIR",   // contains AuthN config and tokens DB
		LogDir:        "AIS_AUTHN_LOG_DIR",
		LogLevel:      "AIS_AUTHN_LOG_LEVEL",
		Port:          "AIS_AUTHN_PORT",
		TTL:           "AIS_AUTHN_TTL",
		UseHTTPS:      "AIS_AUTHN_USE_HTTPS",
		ServerCrt:     "AIS_SERVER_CRT",
		ServerKey:     "AIS_SERVER_KEY",
		SecretKey:     "AIS_AUTHN_SECRET_KEY",
		AdminUsername: "AIS_AUTHN_SU_NAME",
		AdminPassword: "AIS_AUTHN_SU_PASS",
	}
)
