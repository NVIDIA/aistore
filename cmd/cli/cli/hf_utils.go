// Package cli contains shared logic for AIS command-line tools.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"

	"github.com/urfave/cli"
)

// HuggingFace URL patterns and constants
const (
	hfBaseURL         = "https://huggingface.co"
	hfDefaultRevision = "main"
	hfModelType       = "model"
	hfDatasetType     = "dataset"
)

// HuggingFace-specific errors
var (
	errHFMissingModel = errors.New("--hf-model is required when using HuggingFace flags")
	errHFMissingFile  = errors.New("--hf-file is required when using HuggingFace flags")
	errHFInvalidRepo  = errors.New("--hf-repo-type must be 'model' or 'dataset'")
)

// hasHuggingFaceFlags checks if any HuggingFace flags are set
func hasHuggingFaceFlags(c *cli.Context) bool {
	return flagIsSet(c, hfModelFlag) ||
		flagIsSet(c, hfFileFlag) ||
		flagIsSet(c, hfAuthFlag)
}

// validateHuggingFaceFlags validates HuggingFace flag combinations
func validateHuggingFaceFlags(c *cli.Context) error {
	if !hasHuggingFaceFlags(c) {
		return nil
	}

	// If any HF flag is set, model and file are required
	model := parseStrFlag(c, hfModelFlag)
	if model == "" {
		return errHFMissingModel
	}

	file := parseStrFlag(c, hfFileFlag)
	if file == "" {
		return errHFMissingFile
	}

	// Validate repo type
	repoType := parseStrFlag(c, hfRepoTypeFlag)
	if repoType != hfModelType && repoType != hfDatasetType {
		return errHFInvalidRepo
	}

	return nil
}

// buildHuggingFaceURL constructs a HuggingFace download URL from flags
func buildHuggingFaceURL(c *cli.Context) (string, error) {
	if err := validateHuggingFaceFlags(c); err != nil {
		return "", err
	}

	model := parseStrFlag(c, hfModelFlag)
	file := parseStrFlag(c, hfFileFlag)
	revision := parseStrFlag(c, hfRevisionFlag)
	repoType := parseStrFlag(c, hfRepoTypeFlag)

	// Use defaults if not provided
	if revision == "" {
		revision = hfDefaultRevision
	}
	if repoType == "" {
		repoType = hfModelType
	}

	// Build URL based on repo type
	var url string
	if repoType == hfDatasetType {
		url = fmt.Sprintf("%s/datasets/%s/resolve/%s/%s", hfBaseURL, model, revision, file)
	} else {
		url = fmt.Sprintf("%s/%s/resolve/%s/%s", hfBaseURL, model, revision, file)
	}

	return url, nil
}
