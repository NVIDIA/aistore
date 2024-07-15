// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

// assorted gen-purpose constants

const (
	unknownVal = "-"
	NotSetVal  = "-"

	UnknownStatusVal = "n/a"
)

const (
	ClusterTotal = "--- Cluster:"
	TargetTotal  = "------- Sum:"
)

const (
	primarySuffix       = "[P]"
	nonElectableSuffix  = "[n/e]"
	offlineStatusSuffix = "[x]" // (daeStatus, via apc.WhatNodeStatsAndStatus)

	NodeOnline = "online"
)

const (
	xfinished     = "Finished"
	xfinishedErrs = "Finished with errors"
	xrunning      = "Running"
	xidle         = "Idle"
	xaborted      = "Aborted"
)
