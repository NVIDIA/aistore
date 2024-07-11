// Package ishard provides sample extension configs and associated actions
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"fmt"
)

type SampleKeyPattern struct {
	Regex        string
	CaptureGroup string
}

// Define some commonly used sample key patterns
var (
	BaseFileNamePattern   = SampleKeyPattern{Regex: `.*/([^/]+)$`, CaptureGroup: "$1"}
	FullNamePattern       = SampleKeyPattern{Regex: `^.*$`, CaptureGroup: "$0"}
	CollapseAllDirPattern = SampleKeyPattern{Regex: `/`, CaptureGroup: ""}
)

// Action types and functions
func abortAction(missingExt string) error {
	return fmt.Errorf("missing extension: %s. Aborting process", missingExt)
}

func warnAction(missingExt string) error {
	fmt.Printf("Warning: missing extension: %s\n", missingExt)
	return nil
}

func ignoreAction(missingExt string) error {
	fmt.Printf("Ignoring missing extension: %s\n", missingExt)
	return nil
}

type MissingExtFunc func(missingExt string) error

var MissingExtActMap = map[string]MissingExtFunc{
	"abort":  abortAction,
	"warn":   warnAction,
	"ignore": ignoreAction,
}
