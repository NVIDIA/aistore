// Package hf contains HuggingFace integration logic for AIS.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package hf

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// HuggingFace URL patterns and constants
const (
	hfBaseURL         = "https://huggingface.co"
	hfDefaultRevision = "main"
	HfFullRepoMarker  = "HF_FULL_REPO_DOWNLOAD:"
)

// HasHuggingFaceRepoFlags checks if only ModelFlag OR DatasetFlag (not both) is set
func HasHuggingFaceRepoFlags(hasModel, hasDataset bool) bool {
	return hasModel || hasDataset
}

// BuildHuggingFaceURL constructs a HuggingFace download URL from parameters
func BuildHuggingFaceURL(model, dataset, file, revision string) (string, error) {
	// Validate parameters
	hasModel := model != ""
	hasDataset := dataset != ""

	if hasModel && hasDataset {
		return "", errors.New("model and dataset are mutually exclusive")
	}

	// If file or revision are used, require a repository
	if (file != "" || revision != "") && !hasModel && !hasDataset {
		return "", errors.New("file and revision require either model or dataset")
	}

	// Build base URL (models vs datasets have different URL patterns)
	var baseURL string
	switch {
	case hasModel:
		if model == "" {
			return "", errors.New("model cannot be empty")
		}
		baseURL = fmt.Sprintf("%s/%s", hfBaseURL, model)
	case hasDataset:
		if dataset == "" {
			return "", errors.New("dataset cannot be empty")
		}
		baseURL = fmt.Sprintf("%s/datasets/%s", hfBaseURL, dataset)
	default:
		return "", errors.New("either model or dataset must be specified")
	}

	// Use default revision if not provided
	if revision == "" {
		revision = hfDefaultRevision
	}

	// Build final URL
	if file == "" {
		// Return special marker to indicate full repository download needed
		return HfFullRepoMarker + baseURL + ":" + revision, nil
	}
	return fmt.Sprintf("%s/resolve/%s/%s", baseURL, revision, file), nil
}

// IsHuggingFaceURL checks if a URL is a HuggingFace URL
func IsHuggingFaceURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(u.Host, "huggingface.co")
}
