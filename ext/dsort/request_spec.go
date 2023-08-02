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
		cfg  = cmn.GCO.Get().DSort
		pars = &ParsedRequestSpec{}
	)

	if rs.InputBck.Name == "" {
		return pars, errMissingSrcBucket
	}
	if rs.InputBck.Provider == "" {
		rs.InputBck.Provider = apc.AIS
	}
	if _, err := cmn.NormalizeProvider(rs.InputBck.Provider); err != nil {
		return pars, err
	}
	if err := rs.InputBck.Validate(); err != nil {
		return pars, err
	}
	pars.Description = rs.Description
	pars.Bck = rs.InputBck
	pars.OutputBck = rs.OutputBck
	if pars.OutputBck.IsEmpty() {
		pars.OutputBck = pars.Bck
	} else if _, err := cmn.NormalizeProvider(rs.OutputBck.Provider); err != nil {
		return pars, err
	} else if err := rs.OutputBck.Validate(); err != nil {
		return pars, err
	}

	var err error
	pars.Pit, err = parseInputFormat(rs.InputFormat)
	if err != nil {
		return nil, err
	}

	if rs.Extension != "" {
		pars.Extension, err = archive.Mime(rs.Extension, "")
		if err != nil {
			return nil, err
		}
	}

	pars.OutputShardSize, err = cos.ParseSize(rs.OutputShardSize, cos.UnitsIEC)
	if err != nil {
		return nil, err
	}
	if pars.OutputShardSize < 0 {
		return nil, fmt.Errorf(fmtErrNegOutputSize, pars.OutputShardSize)
	}

	pars.Algorithm, err = parseAlgorithm(rs.Algorithm)
	if err != nil {
		return nil, err
	}

	empty, err := validateOrderFileURL(rs.OrderFileURL)
	if err != nil {
		return nil, fmt.Errorf(fmtErrOrderURL, rs.OrderFileURL, err)
	}
	if empty {
		if pars.Pot, err = parseOutputFormat(rs.OutputFormat); err != nil {
			return nil, err
		}
		if pars.Pot.Template.Count() > math.MaxInt32 {
			// If the count is not defined the output shard size must be
			if pars.OutputShardSize == 0 {
				return nil, errMissingOutputSize
			}
		}
	} else { // Valid and not empty.
		// For the order file the output shard size must be set.
		if pars.OutputShardSize == 0 {
			return nil, errMissingOutputSize
		}

		pars.OrderFileURL = rs.OrderFileURL

		pars.OrderFileSep = rs.OrderFileSep
		if pars.OrderFileSep == "" {
			pars.OrderFileSep = "\t"
		}
	}

	if rs.MaxMemUsage == "" {
		rs.MaxMemUsage = cfg.DefaultMaxMemUsage
	}
	pars.MaxMemUsage, err = cos.ParseQuantity(rs.MaxMemUsage)
	if err != nil {
		return nil, err
	}
	if rs.ExtractConcMaxLimit < 0 {
		return nil, fmt.Errorf("%w ('extract', %d)", errNegConcLimit, rs.ExtractConcMaxLimit)
	}
	if rs.CreateConcMaxLimit < 0 {
		return nil, fmt.Errorf("%w ('create', %d)", errNegConcLimit, rs.CreateConcMaxLimit)
	}

	pars.ExtractConcMaxLimit = rs.ExtractConcMaxLimit
	pars.CreateConcMaxLimit = rs.CreateConcMaxLimit
	pars.StreamMultiplier = rs.StreamMultiplier
	pars.ExtendedMetrics = rs.ExtendedMetrics
	pars.DSorterType = rs.DSorterType
	pars.DryRun = rs.DryRun

	// Check for values that override the global config.
	if err := rs.Config.ValidateWithOpts(true); err != nil {
		return nil, err
	}
	pars.DSortConf = rs.Config
	if pars.MissingShards == "" {
		pars.MissingShards = cfg.MissingShards
	}
	if pars.EKMMalformedLine == "" {
		pars.EKMMalformedLine = cfg.EKMMalformedLine
	}
	if pars.EKMMissingKey == "" {
		pars.EKMMissingKey = cfg.EKMMissingKey
	}
	if pars.DuplicatedRecords == "" {
		pars.DuplicatedRecords = cfg.DuplicatedRecords
	}
	if pars.DSorterMemThreshold == "" {
		pars.DSorterMemThreshold = cfg.DSorterMemThreshold
	}

	return pars, nil
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
