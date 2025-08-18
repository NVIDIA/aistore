// Package main contains constants used throughout the documentation generator.
//
// Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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
	dash         = "-"
	underscore   = "_"
	newlineChar  = "\n"
	quote        = `"`
	escapedQuote = `\"`

	// Display and formatting constants
	defaultLabel           = "Request"
	bashLang               = "Bash"
	supportedActionsHeader = "Supported actions: "
	actionLabelFormat      = "%s - Available fields: "
	modelExampleFormat     = "%s"
	modelLabelFormat       = "%s - No fields available"
	fieldDetailsNA         = "No fields available"
	actionSeparator        = "; "

	// HTML link templates
	actionLinkFormat = "<a href='../Models/%s.html'>%s</a>"
	apiLinkFormat    = "<a href='../Apis/%sApi.html#%s'>%s</a>"

	// Error and log messages
	errorParsingEndpoint = "Error parsing endpoint: %v\n"
	warningNoComment     = "Warning: no comment for %s\n"
	cleanupMessage       = "Cleaning up temp directory %s\n"
	malformedEndpointErr = "malformed endpoint line"
)
