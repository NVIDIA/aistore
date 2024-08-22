// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
)

const DefaultExt = archive.ExtTar // default shard extension/format/MIME when spec's input_extension is empty

const (
	algDefault   = ""             // default (alphanumeric, increasing)
	Alphanumeric = "alphanumeric" // string comparison (decreasing or increasing)
	None         = "none"         // none (used for resharding)
	MD5          = "md5"          // compare md5(name)
	Shuffle      = "shuffle"      // random shuffle (use with the same seed to reproduce)
	Content      = "content"      // extract (int, string, float) from a given file, and compare
)

var algorithms = []string{algDefault, Alphanumeric, MD5, Shuffle, Content, None}

type Algorithm struct {
	// one of the `algorithms` above
	Kind string `json:"kind"`

	// used with two sorting alg-s: Alphanumeric and Content
	Decreasing bool `json:"decreasing"`

	// when sort is a random shuffle
	Seed string `json:"seed"`

	// usage: exclusively for Content sorting
	// e.g.: ".cls" containing sorting key for each record (sample) - see next
	// NOTE: not to confuse with shards "input_extension"
	Ext string `json:"extension"`

	// ditto: Content only
	// `shard.contentKeyTypes` enum values: {"int", "string", "float" }
	ContentKeyType string `json:"content_key_type"`
}

// RequestSpec defines the user specification for requests to the endpoint /v1/sort.
type RequestSpec struct {
	// Required
	InputBck        cmn.Bck       `json:"input_bck" yaml:"input_bck"`
	InputFormat     apc.ListRange `json:"input_format" yaml:"input_format"`
	OutputFormat    string        `json:"output_format" yaml:"output_format"`
	OutputShardSize string        `json:"output_shard_size" yaml:"output_shard_size"`

	// Desirable
	InputExtension string `json:"input_extension" yaml:"input_extension"`

	// Optional
	// Default: InputExtension
	OutputExtension string `json:"output_extension" yaml:"output_extension"`
	// Default: ""
	Description string `json:"description" yaml:"description"`
	// Default: same as `bck` field
	OutputBck cmn.Bck `json:"output_bck" yaml:"output_bck"`
	// Default: alphanumeric, increasing
	Algorithm Algorithm `json:"algorithm" yaml:"algorithm"`
	// Default: ""
	EKMFileURL string `json:"ekm_file" yaml:"ekm_file"`
	// Default: "\t"
	EKMFileSep string `json:"ekm_file_sep" yaml:"ekm_file_sep"`
	// Default: "80%"
	MaxMemUsage string `json:"max_mem_usage" yaml:"max_mem_usage"`
	// Default: calcMaxLimit()
	ExtractConcMaxLimit int `json:"extract_concurrency_max_limit" yaml:"extract_concurrency_max_limit"`
	// Default: calcMaxLimit()
	CreateConcMaxLimit int `json:"create_concurrency_max_limit" yaml:"create_concurrency_max_limit"`

	// debug
	DsorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"` // Default: false

	Config cmn.DsortConf
}
