// Package authn provides AuthN API over HTTP(S)
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package authn

// authn environment variable names (see also cmn/env_const.go)

var (
	EnvVars = struct {
		Enabled   string
		URL       string
		ConfFile  string
		TokenFile string
		ConfDir   string
		LogDir    string
		LogLevel  string
		Port      string
		TTL       string
		UseHTTPS  string
	}{
		Enabled:   "AIS_AUTHN_ENABLED",
		URL:       "AIS_AUTHN_URL",
		ConfFile:  "AIS_AUTHN_CONF_FILE",  // fully qualified
		TokenFile: "AIS_AUTHN_TOKEN_FILE", // ditto
		ConfDir:   "AIS_AUTHN_CONF_DIR",
		LogDir:    "AIS_AUTHN_LOG_DIR",
		LogLevel:  "AIS_AUTHN_LOG_LEVEL",
		Port:      "AIS_AUTHN_PORT",
		TTL:       "AIS_AUTHN_TTL",
		UseHTTPS:  "AIS_AUTHN_USE_HTTPS",
	}
)
