/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
)

const (
	// ExtTar is tar files extension
	ExtTar = ".tar"
	// ExtTgz is short tar tgz files extension
	ExtTgz = ".tgz"
	// ExtTarTgz is tar tgz files extension
	ExtTarTgz = ".tar.gz"
	// ExtZip is zip files extension
	ExtZip = ".zip"

	templBash = "bash"
	templAt   = "@"
)

var (
	errMissingBucket            = errors.New("missing field 'bucket'")
	errInvalidExtension         = errors.New("extension must be one of '.tar', '.tar.gz', or '.tgz'")
	errNegOutputShardSize       = errors.New("output shard size must be > 0")
	errNegativeConcurrencyLimit = fmt.Errorf("concurrency limit must be 0 (limits will be calculated) or > 0")

	errInvalidInputTemplateFormat  = errors.New("could not parse given input format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOutputTemplateFormat = errors.New("could not parse given output format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOrderParam           = errors.New("could not parse order format, required URL")

	errInvalidAlgorithm          = errors.New("invalid algorithm specified")
	errInvalidAlgorithmKind      = fmt.Errorf("invalid algorithm kind, should be one of: %+v", supportedAlgorithms)
	errInvalidSeed               = errors.New("invalid seed provided, should be int")
	errInvalidAlgorithmExtension = errors.New("invalid extension provided, should be in format: .ext")
)

var (
	// supportedExtensions is a list of supported extensions by dSort
	supportedExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip}
)

// TODO: maybe this struct should be composed of `type` and `template` where
// template is interface and each template has it's own struct. Then we could
// reflect the interface and based on it start different traverse function.
type parsedInputTemplate struct {
	Type string `json:"type"`

	// Used by 'bash' and 'at' template
	Template cmn.ParsedTemplate `json:"template"`

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
	InputFormat     string `json:"input_format"`
	OutputFormat    string `json:"output_format"`
	OutputShardSize string `json:"output_shard_size"`

	// Optional
	Description      string        `json:"description"`
	OutputBucket     string        `json:"output_bucket"`             // Default: same as `bucket` field
	Algorithm        SortAlgorithm `json:"algorithm"`                 // Default: alphanumeric, increasing
	OrderFileURL     string        `json:"order_file"`                // Default: ""
	OrderFileSep     string        `json:"order_file_sep"`            // Default: "\t"
	MaxMemUsage      string        `json:"max_mem_usage"`             // Default: "80%"
	Provider         string        `json:"provider"`                  // Default: "ais"
	OutputProvider   string        `json:"output_provider"`           // Default: "ais"
	ExtractConcLimit int           `json:"extract_concurrency_limit"` // Default: DefaultConcLimit
	CreateConcLimit  int           `json:"create_concurrency_limit"`  // Default: DefaultConcLimit
	StreamMultiplier int           `json:"stream_multiplier"`         // Default: transport.IntraBundleMultiplier
	ExtendedMetrics  bool          `json:"extended_metrics"`          // Default: false

	// debug
	DSorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"` // Default: false
}

type ParsedRequestSpec struct {
	Bucket           string                `json:"bucket"`
	Description      string                `json:"description"`
	OutputBucket     string                `json:"output_bucket"`
	Provider         string                `json:"provider"`
	OutputProvider   string                `json:"output_provider"`
	Extension        string                `json:"extension"`
	OutputShardSize  int64                 `json:"output_shard_size,string"`
	InputFormat      *parsedInputTemplate  `json:"input_format"`
	OutputFormat     *parsedOutputTemplate `json:"output_format"`
	Algorithm        *SortAlgorithm        `json:"algorithm"`
	OrderFileURL     string                `json:"order_file"`
	OrderFileSep     string                `json:"order_file_sep"`
	MaxMemUsage      cmn.ParsedQuantity    `json:"max_mem_usage"`
	TargetOrderSalt  []byte                `json:"target_order_salt"`
	ExtractConcLimit int                   `json:"extract_concurrency_limit"` // TODO: should be removed
	CreateConcLimit  int                   `json:"create_concurrency_limit"`  // TODO: should be removed
	StreamMultiplier int                   `json:"stream_multiplier"`         // TODO: should be removed
	ExtendedMetrics  bool                  `json:"extended_metrics"`

	// debug
	DSorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"`
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
	parsedRS.Description = rs.Description
	parsedRS.Bucket = rs.Bucket
	parsedRS.OutputBucket = rs.OutputBucket
	if parsedRS.OutputBucket == "" {
		parsedRS.OutputBucket = parsedRS.Bucket
	}
	parsedRS.Provider = rs.Provider
	if parsedRS.Provider == "" {
		parsedRS.Provider = cmn.AIS
	}
	parsedRS.OutputProvider = rs.OutputProvider
	if parsedRS.OutputProvider == "" {
		parsedRS.OutputProvider = cmn.AIS
	}

	var err error
	parsedRS.InputFormat, err = parseInputFormat(rs.InputFormat)
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

	parsedRS.Algorithm, err = parseAlgorithm(rs.Algorithm)
	if err != nil {
		return nil, errInvalidAlgorithm
	}

	if empty, valid := validateOrderFileURL(rs.OrderFileURL); !valid {
		return nil, errInvalidOrderParam
	} else if empty {
		if parsedRS.OutputFormat, err = parseOutputFormat(rs.OutputFormat); err != nil {
			return nil, err
		}
	} else { // valid and not empty
		parsedRS.OrderFileURL = rs.OrderFileURL

		parsedRS.OrderFileSep = rs.OrderFileSep
		if parsedRS.OrderFileSep == "" {
			parsedRS.OrderFileSep = "\t"
		}
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
	parsedRS.StreamMultiplier = rs.StreamMultiplier
	parsedRS.ExtendedMetrics = rs.ExtendedMetrics
	parsedRS.DSorterType = rs.DSorterType
	parsedRS.DryRun = rs.DryRun
	return parsedRS, nil
}

// validateExtension checks if extension is supported by dsort
func validateExtension(ext string) bool {
	return cmn.StringInSlice(ext, supportedExtensions)
}

// parseInputFormat checks if input format was specified correctly
func parseInputFormat(inputFormat string) (pit *parsedInputTemplate, err error) {
	pit = &parsedInputTemplate{}
	template := strings.TrimSpace(inputFormat)
	if pit.Template, err = cmn.ParseBashTemplate(template); err == nil {
		pit.Type = templBash
	} else if pit.Template, err = cmn.ParseAtTemplate(template); err == nil {
		pit.Type = templAt
	} else {
		return nil, errInvalidInputTemplateFormat
	}

	return
}

// parseOutputFormat checks if output format was specified correctly
func parseOutputFormat(outputFormat string) (pot *parsedOutputTemplate, err error) {
	pot = &parsedOutputTemplate{}
	template := strings.TrimSpace(outputFormat)
	if pot.Template, err = cmn.ParseBashTemplate(template); err == nil {
		// Pass
	} else if pot.Template, err = cmn.ParseAtTemplate(template); err == nil {
		// Pass
	} else {
		return nil, errInvalidOutputTemplateFormat
	}

	return
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

func validateOrderFileURL(orderURL string) (empty, valid bool) {
	if orderURL == "" {
		return true, true
	}

	_, err := url.ParseRequestURI(orderURL)
	return false, err == nil
}
