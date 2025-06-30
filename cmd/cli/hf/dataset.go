// Package hf contains HuggingFace integration logic for AIS.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package hf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	hfAPITimeout    = 30 * time.Second
	hfAPIBaseURL    = "https://huggingface.co/api/datasets"
	hfParquetSuffix = "/parquet"
)

// GetHFDatasetParquetFiles fetches parquet file mappings for a HuggingFace dataset
func GetHFDatasetParquetFiles(dataset, token string) (cos.StrKVs, error) {
	url := hfAPIBaseURL + "/" + dataset + hfParquetSuffix

	client := &http.Client{Timeout: hfAPITimeout}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	if token != "" {
		req.Header.Set(apc.HdrAuthorization, apc.AuthenticationTypeBearer+" "+token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch dataset info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dataset '%s' not accessible (HTTP %d)", dataset, resp.StatusCode)
	}

	data := make(map[string]any)
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to parse dataset API response: %v", err)
	}

	files := ExtractParquetFiles(data)
	if len(files) == 0 {
		return nil, fmt.Errorf("no parquet files found for dataset '%s'", dataset)
	}

	return files, nil
}

// ExtractParquetFiles parses the JSON structure from HF API
func ExtractParquetFiles(data map[string]any) cos.StrKVs {
	files := make(cos.StrKVs)

	for configName, configData := range data {
		configMap, ok := configData.(map[string]any)
		if !ok {
			continue
		}

		for splitName, splitData := range configMap {
			urlList, ok := splitData.([]any)
			if !ok {
				continue
			}

			for i, urlData := range urlList {
				if urlStr, ok := urlData.(string); ok {
					// Create unique object name: config/split/filename
					var objName string
					if len(urlList) == 1 {
						objName = fmt.Sprintf("%s/%d.parquet", splitName, i)
					} else {
						objName = fmt.Sprintf("%s/%s/%d.parquet", configName, splitName, i)
					}
					files[objName] = urlStr
				}
			}
		}
	}

	return files
}

// ExtractDatasetFromHFMarker parses dataset name from hfFullRepoMarker
func ExtractDatasetFromHFMarker(marker string) (string, error) {
	// Parse: "HF_FULL_REPO_DOWNLOAD:https://huggingface.co/datasets/rajpurkar/squad:main"
	parts := strings.Split(marker, ":")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid marker format: expected at least 4 parts, got %d", len(parts))
	}

	hfURL := strings.Join(parts[1:], ":")

	switch {
	case !strings.Contains(hfURL, "/datasets/"):
		return "", errors.New("marker contains model URL, expected dataset URL")
	case !strings.HasPrefix(hfURL, hfBaseURL):
		return "", fmt.Errorf("invalid HuggingFace URL: expected prefix %s, got %s", hfBaseURL, hfURL)
	default:
		urlParts := strings.Split(hfURL, "/datasets/")
		if len(urlParts) != 2 {
			return "", fmt.Errorf("invalid dataset URL format: %s", hfURL)
		}

		dataset := urlParts[1]
		if idx := strings.LastIndex(dataset, ":"); idx > 0 {
			dataset = dataset[:idx] // Remove revision suffix
		}

		if dataset == "" {
			return "", fmt.Errorf("empty dataset name extracted from URL: %s", hfURL)
		}

		return dataset, nil
	}
}
