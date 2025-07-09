// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"

	jsoniter "github.com/json-iterator/go"
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
	if specArg == fileStdIO {
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
func parseSpec(ext string, specBytes []byte, spec any) error {
	if ext != "" {
		ext = strings.ToLower(ext)
	}
	if ext == ".json" || ext == ".jsonc" || ext == ".js" {
		if errj := jsoniter.Unmarshal(specBytes, spec); errj != nil {
			if erry := yaml.Unmarshal(specBytes, spec); erry != nil {
				return fmt.Errorf("failed to parse %s file, errs: (%v, %v)", specFlag, errj, erry)
			}
		}
	} else {
		if erry := yaml.Unmarshal(specBytes, spec); erry != nil {
			if errj := jsoniter.Unmarshal(specBytes, spec); errj != nil {
				return fmt.Errorf("failed to parse %s file, errs: (%v, %v)", specFlag, erry, errj)
			}
		}
	}
	return nil
}

func isInlineSpec(arg string) bool {
	if arg == fileStdIO {
		return false
	}

	// check for inline JSON/YAML patterns
	trimmed := strings.TrimSpace(arg)
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "-")
}
