/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
)

const (
	// extTar is tar files extension
	extTar = ".tar"
	// extTgz is short tar tgz files extension
	extTgz = ".tgz"
	// extTarTgz is tar tgz files extension
	extTarTgz = ".tar.gz"
	// extZip is zip files extension
	extZip = ".zip"

	templBash = "bash"
	templAt   = "@"
)

var (
	errMissingBucket            = errors.New("missing field 'bucket'")
	errInvalidExtension         = errors.New("extension must be one of '.tar', '.tar.gz', or '.tgz'")
	errNegOutputShardSize       = errors.New("output shard size must be > 0")
	errNegativeConcurrencyLimit = fmt.Errorf("concurrency limit must be 0 (limits will be calculated) or > 0")

	errInvalidInputFormat  = errors.New("could not parse given input format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOutputFormat = errors.New("could not parse given output format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")

	errInvalidAlgorithm          = errors.New("invalid algorithm specified")
	errInvalidAlgorithmKind      = fmt.Errorf("invalid algorithm kind, should be one of: %+v", supportedAlgorithms)
	errInvalidSeed               = errors.New("invalid seed provided, should be int")
	errInvalidAlgorithmExtension = errors.New("invalid extension provided, should be in format: .ext")
)

var (
	// supportedExtensions is a list of supported extensions by dsort
	supportedExtensions = []string{extTar, extTgz, extTarTgz, extZip}
)

// TODO: maybe this struct should be composed of `type` and `template` where
// template is interface and each template has it's own struct. Then we could
// reflect the interface and based on it start different traverse function.
type parsedInputTemplate struct {
	Type string `json:"type"`

	// Used by 'bash' and 'at' template
	Template cmn.ParsedTemplate

	// Used by 'regex' template
	Regex string `json:"regex"`

	// Used by 'file' template
	File []string `json:"file"`
}

type parsedOutputTemplate struct {
	// Used by 'bash' and 'at' template
	Template cmn.ParsedTemplate
}

// RequestSpec defines the user specification for requests to the endpoint /v1/sort.
type RequestSpec struct {
	// Required
	Bucket          string `json:"bucket"`
	Extension       string `json:"extension"`
	IntputFormat    string `json:"input_format"`
	OutputFormat    string `json:"output_format"`
	OutputShardSize string `json:"output_shard_size"`

	// Optional
	ProcDescription   string        `json:"description"`
	OutputBucket      string        `json:"output_bucket"`             // Default: same as `bucket` field
	Algorithm         SortAlgorithm `json:"algorithm"`                 // Default: alphanumeric, increasing
	MaxMemUsage       string        `json:"max_mem_usage"`             // Default: "80%"
	BckProvider       string        `json:"bprovider"`                 // Default: "local"
	OutputBckProvider string        `json:"output_bprovider"`          // Default: "local"
	ExtractConcLimit  int64         `json:"extract_concurrency_limit"` // Default: DefaultConcLimit
	CreateConcLimit   int64         `json:"create_concurrency_limit"`  // Default: DefaultConcLimit
	ExtendedMetrics   bool          `json:"extended_metrics"`          // Default: false
}

type ParsedRequestSpec struct {
	Bucket            string                `json:"bucket"`
	ProcDescription   string                `json:"description"`
	OutputBucket      string                `json:"output_bucket"`
	BckProvider       string                `json:"bprovider"`
	OutputBckProvider string                `json:"output_bprovider"`
	Extension         string                `json:"extension"`
	OutputShardSize   int64                 `json:"output_shard_size"`
	InputFormat       *parsedInputTemplate  `json:"input_format"`
	OutputFormat      *parsedOutputTemplate `json:"output_format"`
	Algorithm         *SortAlgorithm        `json:"algorithm"`
	MaxMemUsage       cmn.ParsedQuantity    `json:"max_mem_usage"`
	TargetOrderSalt   []byte                `json:"target_order_salt"`
	ExtractConcLimit  int64                 `json:"extract_concurrency_limit"`
	CreateConcLimit   int64                 `json:"create_concurrency_limit"`
	ExtendedMetrics   bool                  `json:"extended_metrics"`
}

type SortAlgorithm struct {
	Kind string `json:"kind"`

	// Kind: alphanumeric, content
	Decreasing bool `json:"decreasing"`

	// Kind: shuffle
	Seed string `json:"seed"` // seed provided to random generator

	// Kind: content
	Extension  string `json:"extension"`
	FormatType string `json:"format_type"`
}

// Parse returns a non-nil error if a RequestSpec is invalid. When RequestSpec
// is valid it parses all the fields, sets the values and returns ParsedRequestSpec.
func (rs *RequestSpec) Parse() (*ParsedRequestSpec, error) {
	parsedRS := &ParsedRequestSpec{}
	if rs.Bucket == "" {
		return parsedRS, errMissingBucket
	}
	parsedRS.ProcDescription = rs.ProcDescription
	parsedRS.Bucket = rs.Bucket
	parsedRS.OutputBucket = rs.OutputBucket
	if parsedRS.OutputBucket == "" {
		parsedRS.OutputBucket = parsedRS.Bucket
	}
	parsedRS.BckProvider = rs.BckProvider
	if parsedRS.BckProvider == "" {
		parsedRS.BckProvider = cmn.LocalBs
	}
	parsedRS.OutputBckProvider = rs.OutputBckProvider
	if parsedRS.OutputBckProvider == "" {
		parsedRS.OutputBckProvider = cmn.LocalBs
	}

	var err error
	parsedRS.InputFormat, err = parseInputFormat(rs.IntputFormat)
	if err != nil {
		return nil, err
	}

	if !validateExtension(rs.Extension) {
		return nil, errInvalidExtension
	}
	parsedRS.Extension = rs.Extension

	parsedRS.OutputShardSize, err = cmn.S2B(rs.OutputShardSize)
	if err != nil {
		return nil, err
	}
	if parsedRS.OutputShardSize <= 0 {
		return nil, errNegOutputShardSize
	}

	parsedRS.OutputFormat, err = parseOutputFormat(rs.OutputFormat)
	if err != nil {
		return nil, err
	}

	parsedRS.Algorithm, err = parseAlgorithm(rs.Algorithm)
	if err != nil {
		return nil, errInvalidAlgorithm
	}

	if rs.MaxMemUsage == "" {
		rs.MaxMemUsage = cmn.GCO.Get().DSort.DefaultMaxMemUsage
	}

	parsedRS.MaxMemUsage, err = cmn.ParseQuantity(rs.MaxMemUsage)
	if err != nil {
		return nil, err
	}

	if rs.ExtractConcLimit < 0 {
		return nil, errNegativeConcurrencyLimit
	}
	if rs.CreateConcLimit < 0 {
		return nil, errNegativeConcurrencyLimit
	}

	parsedRS.ExtractConcLimit = rs.ExtractConcLimit
	parsedRS.CreateConcLimit = rs.CreateConcLimit
	parsedRS.ExtendedMetrics = rs.ExtendedMetrics
	return parsedRS, nil
}

// validateExtension checks if extension is supported by dsort
func validateExtension(ext string) bool {
	return cmn.StringInSlice(ext, supportedExtensions)
}

// parseInputFormat checks if input format was specified correctly
func parseInputFormat(inputFormat string) (pit *parsedInputTemplate, err error) {
	template := strings.TrimSpace(inputFormat)
	pt := &parsedInputTemplate{}
	if pt.Template, err = cmn.ParseBashTemplate(template); err == nil {
		pt.Type = templBash
	} else if pt.Template, err = cmn.ParseAtTemplate(template); err == nil {
		pt.Type = templAt
	} else {
		return nil, errInvalidInputFormat
	}

	return pt, nil
}

// parseOutputFormat checks if output format was specified correctly
func parseOutputFormat(outputFormat string) (pot *parsedOutputTemplate, err error) {
	template := strings.TrimSpace(outputFormat)
	pt := &parsedOutputTemplate{}
	if pt.Template, err = cmn.ParseBashTemplate(template); err == nil {
		// Pass
	} else if pt.Template, err = cmn.ParseAtTemplate(template); err == nil {
		// Pass
	} else {
		return nil, errInvalidOutputFormat
	}

	return pt, nil
}

func parseAlgorithm(algo SortAlgorithm) (parsedAlgo *SortAlgorithm, err error) {
	if !cmn.StringInSlice(algo.Kind, supportedAlgorithms) {
		return nil, errInvalidAlgorithmKind
	}

	if algo.Seed != "" {
		if value, err := strconv.ParseInt(algo.Seed, 10, 64); value < 0 || err != nil {
			return nil, errInvalidSeed
		}
	}

	if algo.Kind == SortKindContent {
		algo.Extension = strings.TrimSpace(algo.Extension)
		if algo.Extension == "" {
			return nil, errInvalidAlgorithmExtension
		}

		if algo.Extension[0] != '.' { // extension should begin with dot: .cls
			return nil, errInvalidAlgorithmExtension
		}

		if err := extract.ValidateAlgorithmFormatType(algo.FormatType); err != nil {
			return nil, err
		}
	} else {
		algo.FormatType = extract.FormatTypeString
	}

	return &algo, nil
}
