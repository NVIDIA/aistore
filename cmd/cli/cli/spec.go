// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
	"gopkg.in/yaml.v3"
)

const (
	maxSpecSize  = cos.MiB
	dfltSpecSize = 4 * cos.KiB
)

// load a specification from the '--spec' flag
// return raw bytes for further decoding
func loadSpec(c *cli.Context) ([]byte, string /*ext*/, error) {
	if !flagIsSet(c, specFlag) {
		return nil, "", fmt.Errorf("flag %s must be specified", qflprn(specFlag))
	}
	specArg := parseStrFlag(c, specFlag)

	// if it's inline JSON/YAML
	if isInlineSpec(specArg) {
		return []byte(specArg), "", nil
	}

	var r io.Reader
	if specArg == stdInOut {
		r = os.Stdin
	} else {
		f, err := os.Open(specArg)
		if err != nil {
			return nil, "", err
		}
		defer f.Close()
		r = f
	}

	var (
		b   bytes.Buffer
		err error
	)
	b.Grow(dfltSpecSize)

	_, err = io.CopyN(&b, r, maxSpecSize)
	if err == nil {
		// from standard lib: "On return, written == n if and only if err == nil."
		return nil, "", fmt.Errorf("spec file too big (max %s)", teb.FmtSize(maxSpecSize, "", 0))
	}
	if err != io.EOF {
		return nil, "", err
	}
	return b.Bytes(), cos.Ext(specArg), nil
}

// parse JSON or YAML bytes into the provided spec structure
// JSON first w/ fall back to YAML
func parseSpec(ext string, specBytes []byte, spec any) (err error) {
	if ext != "" {
		ext = strings.ToLower(ext)
	}

	// Detect if content looks like JSON (starts with { or [)
	trimmed := strings.TrimSpace(string(specBytes))
	isJSON := strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")

	if isJSON || ext == ".json" || ext == ".jsonc" || ext == ".js" {
		return parseJSONWithValidation(specBytes, spec)
	}

	// For YAML, try YAML first, then JSON as fallback
	if err = yaml.Unmarshal(specBytes, spec); err == nil {
		return nil
	}
	if err = parseJSONWithValidation(specBytes, spec); err == nil {
		return nil
	}
	return fmt.Errorf("failed to parse %s file, error: %v", specFlag, err)
}

// parseJSONWithValidation parses JSON with strict validation that fails on unknown fields
func parseJSONWithValidation(specBytes []byte, spec any) error {
	// Use strict JSON decoder that disallows unknown fields
	decoder := json.NewDecoder(bytes.NewReader(specBytes))
	decoder.DisallowUnknownFields()

	return decoder.Decode(spec)
}

func isInlineSpec(arg string) bool {
	if arg == stdInOut {
		return false
	}

	// check for inline JSON/YAML patterns
	trimmed := strings.TrimSpace(arg)
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "-")
}
