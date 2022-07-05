// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// Filename Constants:
//    - AIS metadata
//    - CLI and AuthN configuration files (defaults)
//    - Authentication Token (default)
//    - persistent markers

// See also: env/* for common environment variables

const (
	GlobalConfigFname   = ".ais.conf"
	OverrideConfigFname = ".ais.override_config"
	ProxyIDFname        = ".ais.proxy_id"

	SmapFname        = ".ais.smap"        // Smap persistent file basename
	RmdFname         = ".ais.rmd"         // rmd persistent file basename
	BmdFname         = ".ais.bmd"         // bmd persistent file basename
	BmdPreviousFname = BmdFname + ".prev" // bmd previous version
	VmdFname         = ".ais.vmd"         // vmd persistent file basename
	EmdFname         = ".ais.emd"         // emd persistent file basename

	CliConfigFname   = "cli.json" // see jsp/app.go
	AuthNConfigFname = "authn.json"
	TokenFname       = "auth.token"

	ShutdownMarker      = ".ais.shutdown"
	MarkersDirName      = ".ais.markers"
	ResilverMarker      = "resilver"
	RebalanceMarker     = "rebalance"
	NodeRestartedMarker = "node_restarted"
)
