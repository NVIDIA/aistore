// Package cli contains shared logic for AIS command-line tools.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/urfave/cli"
)

// HuggingFace URL patterns and constants
const (
	hfBaseURL         = "https://huggingface.co"
	hfDefaultRevision = "main"
	// Special marker to indicate full repository download
	hfFullRepoMarker = "HF_FULL_REPO_DOWNLOAD"
)

// only ModelFlag OR DatasetFlag (not both)
func hasHuggingFaceRepoFlags(c *cli.Context) bool {
	return flagIsSet(c, hfModelFlag) || flagIsSet(c, hfDatasetFlag)
}

// buildHuggingFaceURL constructs a HuggingFace download URL from flags
func buildHuggingFaceURL(c *cli.Context) (string, error) {
	// Validate flag combinations
	if flagIsSet(c, hfModelFlag) && flagIsSet(c, hfDatasetFlag) {
		return "", fmt.Errorf("flags %s and %s are mutually exclusive",
			qflprn(hfModelFlag), qflprn(hfDatasetFlag))
	}

	// If file or revision flags are used, require a repository flag
	if (flagIsSet(c, hfFileFlag) || flagIsSet(c, hfRevisionFlag)) && !hasHuggingFaceRepoFlags(c) {
		return "", fmt.Errorf("flags %s and %s require either %s or %s",
			qflprn(hfFileFlag), qflprn(hfRevisionFlag), qflprn(hfModelFlag), qflprn(hfDatasetFlag))
	}

	// Build base URL (models vs datasets have different URL patterns)
	var baseURL string
	if flagIsSet(c, hfModelFlag) {
		repoName := parseStrFlag(c, hfModelFlag)
		if repoName == "" {
			return "", fmt.Errorf("%s cannot be empty", qflprn(hfModelFlag))
		}
		baseURL = fmt.Sprintf("%s/%s", hfBaseURL, repoName)
	} else {
		repoName := parseStrFlag(c, hfDatasetFlag)
		if repoName == "" {
			return "", fmt.Errorf("%s cannot be empty", qflprn(hfDatasetFlag))
		}
		baseURL = fmt.Sprintf("%s/datasets/%s", hfBaseURL, repoName)
	}

	// Get optional parameters
	file := parseStrFlag(c, hfFileFlag)
	revision := parseStrFlag(c, hfRevisionFlag)
	if revision == "" {
		revision = hfDefaultRevision
	}

	// Build final URL
	if file == "" {
		// Return special marker to indicate full repository download needed
		// This will be handled by the download logic to use HuggingFace API
		// to list and download all files in the repository
		return hfFullRepoMarker + ":" + baseURL + ":" + revision, nil
	}
	return fmt.Sprintf("%s/resolve/%s/%s", baseURL, revision, file), nil
}

// isHuggingFaceURL checks if a URL is a HuggingFace URL
func isHuggingFaceURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(u.Host, "huggingface.co")
}
