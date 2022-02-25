// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort/extract"
)

const (
	templBash = "bash"
	templAt   = "@"
)

var (
	errMissingBucket            = errors.New("missing field 'bucket'")
	errInvalidExtension         = errors.New("extension must be one of '.tar', '.tar.gz', or '.tgz'")
	errNegOutputShardSize       = errors.New("output shard size must be >= 0")
	errEmptyOutputShardSize     = errors.New("output shard size must be set (cannot be 0)")
	errNegativeConcurrencyLimit = fmt.Errorf("concurrency max limit must be 0 (limits will be calculated) or > 0")

	errInvalidInputTemplateFormat  = errors.New("could not parse given input format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOutputTemplateFormat = errors.New("could not parse given output format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOrderParam           = errors.New("could not parse order format, required URL")

	errInvalidAlgorithm          = errors.New("invalid algorithm specified")
	errInvalidAlgorithmKind      = fmt.Errorf("invalid algorithm kind, should be one of: %+v", supportedAlgorithms)
	errInvalidSeed               = errors.New("invalid seed provided, should be int")
	errInvalidAlgorithmExtension = errors.New("invalid extension provided, should be in format: .ext")
)

// supportedExtensions is a list of extensions (archives) supported by dSort
var supportedExtensions = cos.ArchExtensions

// TODO: maybe this struct should be composed of `type` and `template` where
// template is interface and each template has it's own struct. Then we could
// reflect the interface and based on it start different traverse function.
type parsedInputTemplate struct {
	Type string `json:"type"`

	// Used by 'bash' and 'at' template
	Template cos.ParsedTemplate `json:"template"`

	// Used by 'regex' template
	Regex string `json:"regex"`

	// Used by 'file' template
	File []string `json:"file"`
}

type parsedOutputTemplate struct {
	// Used by 'bash' and 'at' template
	Template cos.ParsedTemplate
}

// RequestSpec defines the user specification for requests to the endpoint /v1/sort.
type RequestSpec struct {
	// Required
	Bck             cmn.Bck `json:"bck" yaml:"bck"`
	Extension       string  `json:"extension" yaml:"extension"`
	InputFormat     string  `json:"input_format" yaml:"input_format"`
	OutputFormat    string  `json:"output_format" yaml:"output_format"`
	OutputShardSize string  `json:"output_shard_size" yaml:"output_shard_size"`

	// Optional
	Description string `json:"description" yaml:"description"`
	// Default: same as `bck` field
	OutputBck cmn.Bck `json:"output_bck" yaml:"output_bck"`
	// Default: alphanumeric, increasing
	Algorithm SortAlgorithm `json:"algorithm" yaml:"algorithm"`
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

type ParsedRequestSpec struct {
	Bck                 cmn.Bck               `json:"bck"`
	Description         string                `json:"description"`
	OutputBck           cmn.Bck               `json:"output_bck"`
	Extension           string                `json:"extension"`
	OutputShardSize     int64                 `json:"output_shard_size,string"`
	InputFormat         *parsedInputTemplate  `json:"input_format"`
	OutputFormat        *parsedOutputTemplate `json:"output_format"`
	Algorithm           *SortAlgorithm        `json:"algorithm"`
	OrderFileURL        string                `json:"order_file"`
	OrderFileSep        string                `json:"order_file_sep"`
	MaxMemUsage         cos.ParsedQuantity    `json:"max_mem_usage"`
	TargetOrderSalt     []byte                `json:"target_order_salt"`
	ExtractConcMaxLimit int                   `json:"extract_concurrency_max_limit"`
	CreateConcMaxLimit  int                   `json:"create_concurrency_max_limit"`
	StreamMultiplier    int                   `json:"stream_multiplier"` // TODO: should be removed
	ExtendedMetrics     bool                  `json:"extended_metrics"`

	// debug
	DSorterType string `json:"dsorter_type"`
	DryRun      bool   `json:"dry_run"`

	cmn.DSortConf
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
	var (
		cfg      = cmn.GCO.Get().DSort
		parsedRS = &ParsedRequestSpec{}
	)

	if rs.Bck.Name == "" {
		return parsedRS, errMissingBucket
	}
	if rs.Bck.Provider == "" {
		rs.Bck.Provider = apc.ProviderAIS
	}
	if err := rs.Bck.Validate(); err != nil {
		return parsedRS, err
	}
	parsedRS.Description = rs.Description
	parsedRS.Bck = rs.Bck
	parsedRS.OutputBck = rs.OutputBck
	if parsedRS.OutputBck.IsEmpty() {
		parsedRS.OutputBck = parsedRS.Bck
	} else if err := rs.OutputBck.Validate(); err != nil {
		return parsedRS, err
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

	parsedRS.OutputShardSize, err = cos.S2B(rs.OutputShardSize)
	if err != nil {
		return nil, err
	}
	if parsedRS.OutputShardSize < 0 {
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
		if parsedRS.OutputFormat.Template.Count() > math.MaxInt32 {
			// If the count is not defined then the output shard size must be set.
			if parsedRS.OutputShardSize == 0 {
				return nil, errEmptyOutputShardSize
			}
		}
	} else { // Valid and not empty.
		// For the order file the output shard size must be set.
		if parsedRS.OutputShardSize == 0 {
			return nil, errEmptyOutputShardSize
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
		return nil, errNegativeConcurrencyLimit
	}
	if rs.CreateConcMaxLimit < 0 {
		return nil, errNegativeConcurrencyLimit
	}

	parsedRS.ExtractConcMaxLimit = rs.ExtractConcMaxLimit
	parsedRS.CreateConcMaxLimit = rs.CreateConcMaxLimit
	parsedRS.StreamMultiplier = rs.StreamMultiplier
	parsedRS.ExtendedMetrics = rs.ExtendedMetrics
	parsedRS.DSorterType = rs.DSorterType
	parsedRS.DryRun = rs.DryRun

	// Check for values that override the global config.
	if err := rs.DSortConf.ValidateWithOpts(true); err != nil {
		return nil, err
	}
	parsedRS.DSortConf = rs.DSortConf
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

// validateExtension checks if extension is supported by dsort
func validateExtension(ext string) bool {
	return cos.StringInSlice(ext, supportedExtensions)
}

// parseInputFormat checks if input format was specified correctly
func parseInputFormat(inputFormat string) (pit *parsedInputTemplate, err error) {
	pit = &parsedInputTemplate{}
	template := strings.TrimSpace(inputFormat)
	if pit.Template, err = cos.ParseBashTemplate(template); err == nil {
		pit.Type = templBash
	} else if pit.Template, err = cos.ParseAtTemplate(template); err == nil {
		pit.Type = templAt
	} else {
		return nil, errInvalidInputTemplateFormat
	}

	return
}

// parseOutputFormat checks if output format was specified correctly.
func parseOutputFormat(outputFormat string) (pot *parsedOutputTemplate, err error) {
	pot = &parsedOutputTemplate{}
	template := strings.TrimSpace(outputFormat)
	if pot.Template, err = cos.ParseFmtTemplate(template); err == nil {
		// Pass
	} else if pot.Template, err = cos.ParseBashTemplate(template); err == nil {
		// Pass
	} else if pot.Template, err = cos.ParseAtTemplate(template); err == nil {
		// Pass
	} else {
		return nil, errInvalidOutputTemplateFormat
	}

	return
}

func parseAlgorithm(algo SortAlgorithm) (parsedAlgo *SortAlgorithm, err error) {
	if !cos.StringInSlice(algo.Kind, supportedAlgorithms) {
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
