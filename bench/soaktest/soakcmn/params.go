// Package soakcmn provides constants, variables and functions shared across soaktest
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package soakcmn

import "time"

var Params SoakParams

type SoakParams struct {
	// specifying the primary proxy
	IP   string
	Port string

	ReportDir string // directory to write the reports

	Short bool // if true, skips the longer recipes and make default RegPhaseDuration shorter

	KeepTargets  bool // if true, skips the recipes that remove targets
	LocalCleanup bool // if true, disables cleaning up buckets from other soak test runs

	RecDisable     bool             // if true, skips running recipes, can't be true if any of the below are set
	RecSet         map[int]struct{} // numbers of recipes to run, starting at 0, if nil run all of them, if set short is ignored
	NumCycles      int              // numbers of times to cycle through recipes
	RecRegDisable  bool             // if true, skips running regression during recipe
	RecPctCapacity float64          // percentage (0-100) of capacity to use for recipes
	RecMinFilesize int64            // minimum size of file used in recipes
	RecMaxFilesize int64            // maximum size of file used in recipes
	RecPrimWorkers int              // number of workers to run for recipe

	RegPhaseDisable  bool          // if true, skips running pure regression phases, can't be true if any of the below are set
	RegPhaseDuration time.Duration // the amount of time to run pure regression phases for
	RegPctCapacity   float64       // percentage (0-100) of capacity to use for regression
	RegSetupWorkers  int           // number of workers to setup bucket for regression
	RegSetupDuration time.Duration // time spent setting up bucket for regression
	RegMinFilesize   int64         // minimum size of file used in regression
	RegMaxFilesize   int64         // maximum size of file used in regression
	RegWorkers       int           // number of workers to run for regression
	RegInstances     int           // number of instances of regression
}
