// Package fname contains filename constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fname

// See also: env/* for common environment variables

const (
	// aisnode config
	PlaintextInitialConfig = "ais_local.json"
	GlobalConfig           = ".ais.conf"
	OverrideConfig         = ".ais.override_config"

	// proxy aisnode ID
	ProxyID = ".ais.proxy_id"

	// metadata
	Smap        = ".ais.smap"   // Smap persistent file basename
	Rmd         = ".ais.rmd"    // rmd persistent file basename
	Bmd         = ".ais.bmd"    // bmd persistent file basename
	BmdPrevious = Bmd + ".prev" // bmd previous version
	Vmd         = ".ais.vmd"    // vmd persistent file basename
	Emd         = ".ais.emd"    // emd persistent file basename

	// CLI config
	CliConfig = "cli.json" // see jsp/app.go

	// AuthN config and DB
	AuthNConfig = "authn.json"
	AuthDB      = "authn.db"

	// Token
	Token = "auth.token"

	// Markers

	ShutdownMarker = ".ais.shutdown"

	MarkersDir          = ".ais.markers"
	ResilverMarker      = "resilver"
	RebalanceMarker     = "rebalance"
	NodeRestartedMarker = "node_restarted"
)
