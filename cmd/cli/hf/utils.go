// Package hf contains HuggingFace integration logic for AIS.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package hf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// HuggingFace URL patterns and constants
const (
	hfBaseURL         = "https://huggingface.co"
	hfDefaultRevision = "main"
	HfFullRepoMarker  = "HF_FULL_REPO_DOWNLOAD:"
	maxConcurrency    = 20
)

// FileInfo represents a file with its URL and size
type FileInfo struct {
	URL  string
	Size *int64
}

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

// GetFileSizes makes concurrent HEAD requests to get file sizes for HuggingFace files
func GetFileSizes(files cos.StrKVs, authToken string) ([]FileInfo, error) {
	if len(files) == 0 {
		return nil, nil
	}

	// urls[0] must equal filesInfos[0]
	urls := make([]string, 0, len(files))
	for _, url := range files {
		urls = append(urls, url)
	}

	fileInfos := make([]FileInfo, len(urls))
	client := &http.Client{Timeout: 10 * time.Second}

	// concurrency control
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(maxConcurrency)

	for i, url := range urls {
		g.Go(func() error {
			info, err := fetchFileSize(ctx, url, client, authToken)
			if err != nil {
				return err
			}
			fileInfos[i] = info
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return fileInfos, nil
}

// fetchFileSize makes a single HEAD request to get file size
func fetchFileSize(ctx context.Context, fileURL string, client *http.Client, authToken string) (FileInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", fileURL, http.NoBody)
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to create HEAD request for %s: %v", fileURL, err)
	}

	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to get file size for %s: %v", fileURL, err)
	}
	resp.Body.Close()

	var size *int64
	if resp.ContentLength >= 0 {
		size = &resp.ContentLength
	}

	return FileInfo{
		URL:  fileURL,
		Size: size,
	}, nil
}

// ExtractFileName extracts filename from HuggingFace URL
func ExtractFileName(url string) string {
	parts := strings.Split(url, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return "unknown"
}
