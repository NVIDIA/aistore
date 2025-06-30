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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	hfModelsAPIPath = "/api/models"
)

// HFModelResponse represents the HuggingFace model API response
type HFModelResponse struct {
	Siblings []HFModelFile `json:"siblings"`
}

// HFModelFile represents a file in HuggingFace model repository
type HFModelFile struct {
	Filename string `json:"rfilename"`
}

// GetHFModelFiles fetches all files in a HuggingFace model repository
func GetHFModelFiles(model, token string) (cos.StrKVs, error) {
	url := hfBaseURL + hfModelsAPIPath + "/" + model

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
		return nil, fmt.Errorf("failed to fetch model info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("model '%s' not accessible (HTTP %d)", model, resp.StatusCode)
	}

	var modelResp HFModelResponse
	if err := json.NewDecoder(resp.Body).Decode(&modelResp); err != nil {
		return nil, fmt.Errorf("failed to parse model API response: %v", err)
	}

	// Convert to download URLs map
	result := make(cos.StrKVs)
	for _, file := range modelResp.Siblings {
		if strings.HasSuffix(file.Filename, "/") || file.Filename == ".gitattributes" {
			continue
		}
		downloadURL := fmt.Sprintf("%s/%s/resolve/%s/%s", hfBaseURL, model, hfDefaultRevision, file.Filename)
		result[file.Filename] = downloadURL
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no files found for model '%s'", model)
	}

	return result, nil
}

// ExtractModelFromHFMarker parses model name from hfFullRepoMarker
func ExtractModelFromHFMarker(marker string) (string, error) {
	// Parse: "HF_FULL_REPO_DOWNLOAD:https://huggingface.co/bert-base-uncased:main"
	parts := strings.Split(marker, ":")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid marker format: expected at least 4 parts, got %d", len(parts))
	}

	hfURL := strings.Join(parts[1:], ":")

	switch {
	case strings.Contains(hfURL, "/datasets/"):
		return "", errors.New("marker contains dataset URL, expected model URL")
	case !strings.HasPrefix(hfURL, hfBaseURL):
		return "", fmt.Errorf("invalid HuggingFace URL: expected prefix %s, got %s", hfBaseURL, hfURL)
	default:
		// Extract model name
		model := strings.TrimPrefix(hfURL, hfBaseURL+"/")
		if idx := strings.LastIndex(model, ":"); idx > 0 {
			model = model[:idx] // Remove revision suffix
		}

		if model == "" {
			return "", fmt.Errorf("empty model name extracted from URL: %s", hfURL)
		}

		return model, nil
	}
}
