// Package main contains constants used throughout the documentation generator.
//
// Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
package main

const (
	// String parsing and formatting constants
	openBracket  = "["
	closeBracket = "]"
	openParen    = "("
	closeParen   = ")"
	comma        = ","
	equals       = "="
	pipe         = "|"
	dot          = "."
	queryStart   = "?"
	queryJoin    = "&"
	newlineChar  = "\n"

	// Display and formatting constants
	defaultLabel           = "Request"
	bashLang               = "Bash"
	supportedActionsHeader = "Supported actions: "

	// Error and log messages
	errorParsingEndpoint = "Error parsing endpoint: %v\n"
	malformedEndpointErr = "malformed endpoint line"
)
