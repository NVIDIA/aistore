// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
)

type parsedInputTemplate struct {
	Template cos.ParsedTemplate `json:"template"`
	ObjNames []string           `json:"objnames"`
	Prefix   string             `json:"prefix"`
}

type parsedOutputTemplate struct {
	// Used by 'bash' and 'at' template
	Template cos.ParsedTemplate
}

type ParsedRequestSpec struct {
	Bck                 cmn.Bck               `json:"bck"`
	Description         string                `json:"description"`
	OutputBck           cmn.Bck               `json:"output_bck"`
	Extension           string                `json:"extension"`
	OutputShardSize     int64                 `json:"output_shard_size,string"`
	Pit                 *parsedInputTemplate  `json:"pit"`
	Pot                 *parsedOutputTemplate `json:"pot"`
	Algorithm           *Algorithm            `json:"algorithm"`
	OrderFileURL        string                `json:"order_file"`
	OrderFileSep        string                `json:"order_file_sep"`
	MaxMemUsage         cos.ParsedQuantity    `json:"max_mem_usage"`
	TargetOrderSalt     []byte                `json:"target_order_salt"`
	ExtractConcMaxLimit int                   `json:"extract_concurrency_max_limit"`
	CreateConcMaxLimit  int                   `json:"create_concurrency_max_limit"`
	StreamMultiplier    int                   `json:"stream_multiplier"` // TODO: remove
	ExtendedMetrics     bool                  `json:"extended_metrics"`

	// debug
	DSorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"`

	cmn.DSortConf
}

/////////////////
// RequestSpec //
/////////////////

// Parse returns a non-nil error if a RequestSpec is invalid. When RequestSpec
// is valid it parses all the fields, sets the values and returns ParsedRequestSpec.
func (rs *RequestSpec) Parse() (*ParsedRequestSpec, error) {
	var (
		cfg      = cmn.GCO.Get().DSort
		parsedRS = &ParsedRequestSpec{}
	)

	if rs.InputBck.Name == "" {
		return parsedRS, errMissingSrcBucket
	}
	if rs.InputBck.Provider == "" {
		rs.InputBck.Provider = apc.AIS
	}
	if _, err := cmn.NormalizeProvider(rs.InputBck.Provider); err != nil {
		return parsedRS, err
	}
	if err := rs.InputBck.Validate(); err != nil {
		return parsedRS, err
	}
	parsedRS.Description = rs.Description
	parsedRS.Bck = rs.InputBck
	parsedRS.OutputBck = rs.OutputBck
	if parsedRS.OutputBck.IsEmpty() {
		parsedRS.OutputBck = parsedRS.Bck
	} else if _, err := cmn.NormalizeProvider(rs.OutputBck.Provider); err != nil {
		return parsedRS, err
	} else if err := rs.OutputBck.Validate(); err != nil {
		return parsedRS, err
	}

	var err error
	parsedRS.Pit, err = parseInputFormat(rs.InputFormat)
	if err != nil {
		return nil, err
	}

	var ext string
	ext, err = archive.Mime(rs.Extension, "")
	if err != nil {
		return nil, err
	}
	parsedRS.Extension = ext

	parsedRS.OutputShardSize, err = cos.ParseSize(rs.OutputShardSize, cos.UnitsIEC)
	if err != nil {
		return nil, err
	}
	if parsedRS.OutputShardSize < 0 {
		return nil, fmt.Errorf(fmtErrNegOutputSize, parsedRS.OutputShardSize)
	}

	parsedRS.Algorithm, err = parseAlgorithm(rs.Algorithm)
	if err != nil {
		return nil, err
	}

	empty, err := validateOrderFileURL(rs.OrderFileURL)
	if err != nil {
		return nil, fmt.Errorf(fmtErrOrderURL, rs.OrderFileURL, err)
	}
	if empty {
		if parsedRS.Pot, err = parseOutputFormat(rs.OutputFormat); err != nil {
			return nil, err
		}
		if parsedRS.Pot.Template.Count() > math.MaxInt32 {
			// If the count is not defined the output shard size must be
			if parsedRS.OutputShardSize == 0 {
				return nil, errMissingOutputSize
			}
		}
	} else { // Valid and not empty.
		// For the order file the output shard size must be set.
		if parsedRS.OutputShardSize == 0 {
			return nil, errMissingOutputSize
		}

		parsedRS.OrderFileURL = rs.OrderFileURL

		parsedRS.OrderFileSep = rs.OrderFileSep
		if parsedRS.OrderFileSep == "" {
			parsedRS.OrderFileSep = "\t"
		}
	}

	if rs.MaxMemUsage == "" {
		rs.MaxMemUsage = cfg.DefaultMaxMemUsage
	}
	parsedRS.MaxMemUsage, err = cos.ParseQuantity(rs.MaxMemUsage)
	if err != nil {
		return nil, err
	}
	if rs.ExtractConcMaxLimit < 0 {
		return nil, fmt.Errorf("%w ('extract', %d)", errNegConcLimit, rs.ExtractConcMaxLimit)
	}
	if rs.CreateConcMaxLimit < 0 {
		return nil, fmt.Errorf("%w ('create', %d)", errNegConcLimit, rs.CreateConcMaxLimit)
	}

	parsedRS.ExtractConcMaxLimit = rs.ExtractConcMaxLimit
	parsedRS.CreateConcMaxLimit = rs.CreateConcMaxLimit
	parsedRS.StreamMultiplier = rs.StreamMultiplier
	parsedRS.ExtendedMetrics = rs.ExtendedMetrics
	parsedRS.DSorterType = rs.DSorterType
	parsedRS.DryRun = rs.DryRun

	// Check for values that override the global config.
	if err := rs.Config.ValidateWithOpts(true); err != nil {
		return nil, err
	}
	parsedRS.DSortConf = rs.Config
	if parsedRS.MissingShards == "" {
		parsedRS.MissingShards = cfg.MissingShards
	}
	if parsedRS.EKMMalformedLine == "" {
		parsedRS.EKMMalformedLine = cfg.EKMMalformedLine
	}
	if parsedRS.EKMMissingKey == "" {
		parsedRS.EKMMissingKey = cfg.EKMMissingKey
	}
	if parsedRS.DuplicatedRecords == "" {
		parsedRS.DuplicatedRecords = cfg.DuplicatedRecords
	}
	if parsedRS.DSorterMemThreshold == "" {
		parsedRS.DSorterMemThreshold = cfg.DSorterMemThreshold
	}

	return parsedRS, nil
}

func parseAlgorithm(alg Algorithm) (*Algorithm, error) {
	if !cos.StringInSlice(alg.Kind, algorithms) {
		return nil, fmt.Errorf(fmtErrInvalidAlg, algorithms)
	}
	if alg.Seed != "" {
		if value, err := strconv.ParseInt(alg.Seed, 10, 64); value < 0 || err != nil {
			return nil, fmt.Errorf(fmtErrSeed, alg.Seed)
		}
	}
	if alg.Kind == Content {
		alg.Ext = strings.TrimSpace(alg.Ext)
		if alg.Ext == "" || alg.Ext[0] != '.' {
			return nil, fmt.Errorf("%w %q", errAlgExt, alg.Ext)
		}
		if err := extract.ValidateContentKeyT(alg.ContentKeyType); err != nil {
			return nil, err
		}
	} else {
		alg.ContentKeyType = extract.ContentKeyString
	}

	return &alg, nil
}

func validateOrderFileURL(orderURL string) (empty bool, err error) {
	if orderURL == "" {
		return true, nil
	}
	_, err = url.ParseRequestURI(orderURL)
	return
}

//////////////////////////
// parsedOutputTemplate //
//////////////////////////

func parseOutputFormat(outputFormat string) (pot *parsedOutputTemplate, err error) {
	pot = &parsedOutputTemplate{}
	if pot.Template, err = cos.NewParsedTemplate(strings.TrimSpace(outputFormat)); err != nil {
		return
	}
	if len(pot.Template.Ranges) == 0 {
		return nil, fmt.Errorf("invalid output template %q: no ranges (prefix-only output is not supported)",
			outputFormat)
	}
	return
}

/////////////////////////
// parsedInputTemplate //
/////////////////////////

func parseInputFormat(inputFormat apc.ListRange) (pit *parsedInputTemplate, err error) {
	pit = &parsedInputTemplate{}
	if inputFormat.IsList() {
		pit.ObjNames = inputFormat.ObjNames
		return
	}
	pit.Template, err = cos.NewParsedTemplate(inputFormat.Template)

	if err == cos.ErrEmptyTemplate {
		// empty template => empty prefix (match any)
		err = nil
		pit.Prefix = ""
	} else if err == nil && len(pit.Template.Ranges) == 0 {
		// prefix only
		pit.Prefix = pit.Template.Prefix
	}
	return
}

func (pit *parsedInputTemplate) isList() bool   { return len(pit.ObjNames) > 0 }
func (pit *parsedInputTemplate) isRange() bool  { return len(pit.Template.Ranges) > 0 }
func (pit *parsedInputTemplate) isPrefix() bool { return !pit.isList() && !pit.isRange() }
