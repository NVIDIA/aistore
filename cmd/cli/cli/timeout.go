// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import "time"

// see also: xact/api.go

const (
	// e.g. xquery --all
	longClientTimeout = 60 * time.Second

	// list-objects progress; list-objects with --summary
	listObjectsWaitTime = 10 * time.Second

	// default '--refresh' durations and counts
	refreshRateDefault = 5 * time.Second
	refreshRateMinDur  = time.Second
	countDefault       = 1
	countUnlimited     = -1

	logFlushTime = 10 * time.Second // as the name implies

	//  progress bar: when stats stop moving (increasing)
	timeoutNoChange = 10 * time.Second

	// download started
	dloadStartedTime = refreshRateDefault

	// job wait: start printing "."(s)
	wasFast = refreshRateDefault
)
