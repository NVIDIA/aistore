// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	algDefault   = ""             // default (alphanumeric, decreasing)
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

	// exclusively with Content sorting
	// e.g. usage: ".cls" to provide sorting key for each record (sample) - see next
	Ext string `json:"extension"`

	// ditto: Content only
	// `extract.contentKeyTypes` enum values: {"int", "string", "float" }
	ContentKeyType string `json:"content_key_type"`
}

// RequestSpec defines the user specification for requests to the endpoint /v1/sort.
type RequestSpec struct {
	// Required
	Bck             cmn.Bck       `json:"bck" yaml:"bck"`
	Extension       string        `json:"extension" yaml:"extension"`
	InputFormat     apc.ListRange `json:"input_format" yaml:"input_format"`
	OutputFormat    string        `json:"output_format" yaml:"output_format"`
	OutputShardSize string        `json:"output_shard_size" yaml:"output_shard_size"`

	// Optional
	Description string `json:"description" yaml:"description"`
	// Default: same as `bck` field
	OutputBck cmn.Bck `json:"output_bck" yaml:"output_bck"`
	// Default: alphanumeric, increasing
	Algorithm Algorithm `json:"algorithm" yaml:"algorithm"`
	// Default: ""
	OrderFileURL string `json:"order_file" yaml:"order_file"`
	// Default: "\t"
	OrderFileSep string `json:"order_file_sep" yaml:"order_file_sep"`
	// Default: "80%"
	MaxMemUsage string `json:"max_mem_usage" yaml:"max_mem_usage"`
	// Default: calcMaxLimit()
	ExtractConcMaxLimit int `json:"extract_concurrency_max_limit" yaml:"extract_concurrency_max_limit"`
	// Default: calcMaxLimit()
	CreateConcMaxLimit int `json:"create_concurrency_max_limit" yaml:"create_concurrency_max_limit"`
	// Default: bundle.Multiplier
	StreamMultiplier int `json:"stream_multiplier" yaml:"stream_multiplier"`
	// Default: false
	ExtendedMetrics bool `json:"extended_metrics" yaml:"extended_metrics"`

	// debug
	DSorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"` // Default: false

	cmn.DSortConf
}
