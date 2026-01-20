//go:build dsort

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var algorithms = []string{algDefault, Alphanumeric, MD5, Shuffle, Content, None}

type parsedInputTemplate struct {
	Template cos.ParsedTemplate `json:"template"`
	ObjNames []string           `json:"objnames"`
	Prefix   string             `json:"prefix"`
}

type parsedOutputTemplate struct {
	// Used by 'bash' and 'at' template
	Template cos.ParsedTemplate
}

type ParsedReq struct {
	InputBck  cmn.Bck
	OutputBck cmn.Bck
	pars      *parsedReqSpec
}

type parsedReqSpec struct {
	InputBck            cmn.Bck               `json:"input_bck"`
	Description         string                `json:"description"`
	OutputBck           cmn.Bck               `json:"output_bck"`
	InputExtension      string                `json:"input_extension"`
	OutputExtension     string                `json:"output_extension"`
	OutputShardSize     int64                 `json:"output_shard_size,string"`
	Pit                 *parsedInputTemplate  `json:"pit"`
	Pot                 *parsedOutputTemplate `json:"pot"`
	Algorithm           *Algorithm            `json:"algorithm"`
	EKMFileURL          string                `json:"ekm_file"`
	EKMFileSep          string                `json:"ekm_file_sep"`
	MaxMemUsage         cos.ParsedQuantity    `json:"max_mem_usage"`
	TargetOrderSalt     []byte                `json:"target_order_salt"`
	ExtractConcMaxLimit int                   `json:"extract_concurrency_max_limit"`
	CreateConcMaxLimit  int                   `json:"create_concurrency_max_limit"`
	SbundleMult         int                   `json:"bundle_multiplier"`

	// debug
	DsorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"`

	cmn.DsortConf
}
