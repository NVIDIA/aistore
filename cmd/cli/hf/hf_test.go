// Package hf contains HuggingFace integration logic for AIS.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package hf_test

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmd/cli/hf"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// Test constants
const (
	validModel       = "testowner/testmodel"
	validDataset     = "testowner/testdataset"
	validFile        = "config.json"
	invalidProvider  = "invalidprovider"
	hfResolvePattern = "https://huggingface.co/%s/resolve/main/%s"
)

// Test URLs
var (
	validModelURL      = "https://huggingface.co/testowner/testmodel"
	invalidURL         = "https://example.com/invalid"
	validModelMarker   = "HF_FULL_REPO_DOWNLOAD:https://huggingface.co/testowner/testmodel:main"
	validDatasetMarker = "HF_FULL_REPO_DOWNLOAD:https://huggingface.co/datasets/testowner/testdataset:main"
)

// TestBuildHuggingFaceURL tests URL construction
func TestBuildHuggingFaceURL(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		dataset     string
		file        string
		expectedURL string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "model with file",
			model:       validModel,
			file:        validFile,
			expectedURL: fmt.Sprintf(hfResolvePattern, validModel, validFile),
		},
		{
			name:        "dataset full repo",
			dataset:     validDataset,
			expectedURL: "HF_FULL_REPO_DOWNLOAD:https://huggingface.co/datasets/testowner/testdataset:main",
		},
		{
			name:        "both model and dataset (error)",
			model:       validModel,
			dataset:     validDataset,
			expectError: true,
			errorMsg:    "model and dataset are mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, err := hf.BuildHuggingFaceURL(tt.model, tt.dataset, tt.file, "")

			if tt.expectError {
				tassert.Errorf(t, err != nil, "expected error but got none")
				if err != nil && tt.errorMsg != "" {
					tassert.Errorf(t, err.Error() == tt.errorMsg,
						"expected error '%s', got '%v'", tt.errorMsg, err)
				}
			} else {
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, url == tt.expectedURL,
					"expected URL '%s', got '%s'", tt.expectedURL, url)
			}
		})
	}
}

// TestIsHuggingFaceURL tests URL validation
func TestIsHuggingFaceURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"valid HF URL", validModelURL, true},
		{"invalid URL", invalidURL, false},
		{"malformed URL", "not-a-url", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hf.IsHuggingFaceURL(tt.url)
			tassert.Errorf(t, result == tt.expected,
				"IsHuggingFaceURL(%s) = %v, expected %v", tt.url, result, tt.expected)
		})
	}
}

// TestHasHuggingFaceRepoFlags tests flag combination validation
func TestHasHuggingFaceRepoFlags(t *testing.T) {
	tests := []struct {
		name     string
		hasModel bool
		hasData  bool
		expected bool
	}{
		{"has model only", true, false, true},
		{"has dataset only", false, true, true},
		{"has both", true, true, true},
		{"has neither", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hf.HasHuggingFaceRepoFlags(tt.hasModel, tt.hasData)
			tassert.Errorf(t, result == tt.expected,
				"HasHuggingFaceRepoFlags(%v, %v) = %v, expected %v",
				tt.hasModel, tt.hasData, result, tt.expected)
		})
	}
}

// TestExtractModelFromHFMarker tests model name extraction from markers
func TestExtractModelFromHFMarker(t *testing.T) {
	tests := []struct {
		name        string
		marker      string
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "valid model marker",
			marker:   validModelMarker,
			expected: validModel,
		},
		{
			name:        "dataset marker (should fail)",
			marker:      validDatasetMarker,
			expectError: true,
			errorMsg:    "marker contains dataset URL, expected model URL",
		},
		{
			name:        "invalid marker format",
			marker:      "INVALID:FORMAT",
			expectError: true,
			errorMsg:    "invalid marker format: expected at least 4 parts, got 2",
		},
		{
			name:        "wrong HF URL prefix",
			marker:      "HF_FULL_REPO_DOWNLOAD:https://github.com/model:main",
			expectError: true,
			errorMsg:    "invalid HuggingFace URL: expected prefix https://huggingface.co, got https://github.com/model:main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hf.ExtractModelFromHFMarker(tt.marker)

			if tt.expectError {
				tassert.Errorf(t, err != nil, "expected error but got none")
				if err != nil && tt.errorMsg != "" {
					tassert.Errorf(t, err.Error() == tt.errorMsg,
						"expected error '%s', got '%v'", tt.errorMsg, err)
				}
			} else {
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, result == tt.expected,
					"expected model '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestExtractDatasetFromHFMarker tests dataset name extraction from markers
func TestExtractDatasetFromHFMarker(t *testing.T) {
	tests := []struct {
		name        string
		marker      string
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "valid dataset marker",
			marker:   validDatasetMarker,
			expected: validDataset,
		},
		{
			name:        "model marker (should fail)",
			marker:      validModelMarker,
			expectError: true,
			errorMsg:    "marker contains model URL, expected dataset URL",
		},
		{
			name:        "invalid marker format",
			marker:      "INVALID:FORMAT:SHORT",
			expectError: true,
			errorMsg:    "invalid marker format: expected at least 4 parts, got 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hf.ExtractDatasetFromHFMarker(tt.marker)

			if tt.expectError {
				tassert.Errorf(t, err != nil, "expected error but got none")
				if err != nil && tt.errorMsg != "" {
					tassert.Errorf(t, err.Error() == tt.errorMsg,
						"expected error '%s', got '%v'", tt.errorMsg, err)
				}
			} else {
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, result == tt.expected,
					"expected dataset '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestExtractParquetFiles tests parquet file extraction from API response
func TestExtractParquetFiles(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]any
		expected cos.StrKVs
	}{
		{
			name: "simple config with train data",
			data: map[string]any{
				"default": map[string]any{
					"train": []any{
						"https://huggingface.co/datasets/squad/resolve/main/data/train-00000.parquet",
					},
				},
			},
			expected: cos.StrKVs{
				"train/0.parquet": "https://huggingface.co/datasets/squad/resolve/main/data/train-00000.parquet",
			},
		},
		{
			name: "multiple files",
			data: map[string]any{
				"config1": map[string]any{
					"train": []any{
						"https://example.com/file1.parquet",
						"https://example.com/file2.parquet",
					},
				},
			},
			expected: cos.StrKVs{
				"config1/train/0.parquet": "https://example.com/file1.parquet",
				"config1/train/1.parquet": "https://example.com/file2.parquet",
			},
		},
		{
			name:     "empty data",
			data:     map[string]any{},
			expected: cos.StrKVs{},
		},
		{
			name: "invalid data structure",
			data: map[string]any{
				"invalid": "not-a-map",
			},
			expected: cos.StrKVs{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hf.ExtractParquetFiles(tt.data)

			tassert.Errorf(t, len(result) == len(tt.expected),
				"expected %d files, got %d", len(tt.expected), len(result))

			for key, expectedURL := range tt.expected {
				actualURL, exists := result[key]
				tassert.Errorf(t, exists, "expected key '%s' not found", key)
				tassert.Errorf(t, actualURL == expectedURL,
					"for key '%s': expected URL '%s', got '%s'", key, expectedURL, actualURL)
			}
		})
	}
}
