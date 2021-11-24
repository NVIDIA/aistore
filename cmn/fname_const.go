// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// file basenames for storing AIS metadata and markers
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

	ShutdownMarker      = ".ais.shutdown"
	MarkersDirName      = ".ais.markers"
	ResilverMarker      = "resilver"
	RebalanceMarker     = "rebalance"
	NodeRestartedMarker = "node_restarted"
)
