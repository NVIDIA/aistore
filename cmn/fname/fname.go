// Package fname contains filename constants and common system directories
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fname

// See also: api/env for common environment variables

const (
	HomeConfigsDir = ".config" // join(cos.HomeDir(), HomeConfigsDir)
	HomeAIS        = "ais"     // join(cos.HomeDir(), HomeConfigsDir, HomeAisDir)
	HomeCLI        = "cli"     // ditto
	HomeAuthN      = "authn"
)

const (
	// plain-text initial configs (NOTE: read-only, never change)
	PlainGlobalConfig = "ais.json"
	PlainLocalConfig  = "ais_local.json"

	// versioned, replicated, and checksum-protected
	GlobalConfig   = ".ais.conf"
	OverrideConfig = ".ais.override_config"

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

	// AuthN: config and DB
	AuthNConfig = "authn.json"
	AuthNDB     = "authn.db"

	// Token
	Token = "auth.token"

	// Markers: per mountpath

	// TODO add the two distinct "skipped" markers:
	//   - ResilverSkippedMarker
	//   - RebalanceSkippedMarker
	// Semantics:
	//   * written when the respective operation is intentionally skipped
	//     (eg. `--no-resilver`, `--no-rebalance`, config-disabled, or equivalent admin choice)
	//   * must not be interpreted as "interrupted", "failed", or "in-progress"
	//   * e.g. usage: SingleRmiJogger

	MarkersDir          = ".ais.markers"
	ResilverMarker      = "resilver"
	RebalanceMarker     = "rebalance"
	NodeRestartedMarker = "node_restarted"
	NodeRestartedPrev   = "node_restarted.prev"
)
