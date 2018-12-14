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
	"unicode"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort/extract"
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

	memPercent = "percent"
	memNumber  = "number"

	// defaultConcLimit determines default concurrency limit when it is not set.
	defaultConcLimit = 100
)

var (
	errMissingBucket            = errors.New("missing field 'bucket'")
	errInvalidExtension         = errors.New("extension must be one of '.tar', '.tar.gz', or '.tgz'")
	errNegOutputShardSize       = errors.New("output shard size must be > 0")
	errNegativeConcurrencyLimit = fmt.Errorf("concurrency limit must be 0 (default: %d) or > 0", defaultConcLimit)

	errInvalidInputFormat  = errors.New("could not parse given input format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")
	errInvalidOutputFormat = errors.New("could not parse given output format, example of bash format: 'prefix{0001..0010}suffix`, example of at format: 'prefix@00100suffix`")

	errInvalidBashFormat = errors.New("input 'bash' format is invalid, should be 'prefix{0001..0010}suffix`")
	errInvalidAtFormat   = errors.New("input 'at' format is invalid, should be 'prefix@00100suffix`")
	errStartAfterEnd     = errors.New("'start' cannot be greater than 'end'")
	errNegativeStart     = errors.New("'start' is negative")
	errNonPositiveStep   = errors.New("'step' is non positive number")

	errInvalidMemUsage      = errors.New("invalid memory usage specified, format should be '81%' or '1GB'")
	errInvalidMaxMemPercent = errors.New("max memory usage percent must be 0 (defaults to 80%) or in the range [1, 99]")
	errInvalidMaxMemNumber  = errors.New("max memory usage value must be non-negative")

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
	Prefix     string `json:"prefix"`
	Start      int    `json:"start"`
	End        int    `json:"end"`
	Step       int    `json:"step"`
	RangeCount int    `json:"range_count"`
	Suffix     string `json:"suffix"`
	DigitCount int    `json:"digit_count"`

	// Used by 'regex' template
	Regex string `json:"regex"`

	// Used by 'file' template
	File []string `json:"file"`
}

type parsedOutputTemplate struct {
	// Used by 'bash' and 'at' template
	Prefix     string `json:"prefix"`
	Start      int    `json:"start"`
	End        int    `json:"end"`
	Step       int    `json:"step"`
	RangeCount int    `json:"range_count"`
	Suffix     string `json:"suffix"`
	DigitCount int    `json:"digit_count"`
}

type parsedMemUsage struct {
	Type  string `json:"type"`
	Value uint64 `json:"value"`
}

// RequestSpec defines the user specification for requests to the endpoint /v1/sort.
type RequestSpec struct {
	// Required
	Bucket       string `json:"bucket"`
	Extension    string `json:"extension"`
	IntputFormat string `json:"input_format"`
	OutputFormat string `json:"output_format"`

	// Optional
	OutputShardSize    int64         `json:"shard_size"`
	IgnoreMissingFiles bool          `json:"ignore_missing_files"`      // Default: false
	Algorithm          SortAlgorithm `json:"algorithm"`                 // Default: alphanumeric, increasing
	MaxMemUsage        string        `json:"max_mem_usage"`             // Default: "80%"
	IsLocalBucket      bool          `json:"local"`                     // Default: false
	ExtractConcLimit   int           `json:"extract_concurrency_limit"` // Default: DefaultConcLimit
	CreateConcLimit    int           `json:"create_concurrency_limit"`  // Default: DefaultConcLimit
}

type ParsedRequestSpec struct {
	Bucket             string                `json:"bucket"`
	Extension          string                `json:"extension"`
	OutputShardSize    int64                 `json:"shard_size"`
	InputFormat        *parsedInputTemplate  `json:"input_format"`
	OutputFormat       *parsedOutputTemplate `json:"output_format"`
	IgnoreMissingFiles bool                  `json:"ignore_missing_files"`
	Algorithm          *SortAlgorithm        `json:"algorithm"`
	MaxMemUsage        *parsedMemUsage       `json:"max_mem_usage"`
	IsLocalBucket      bool                  `json:"local"`
	TargetOrderSalt    []byte                `json:"target_order_salt"`
	ExtractConcLimit   int                   `json:"extract_concurrency_limit"`
	CreateConcLimit    int                   `json:"create_concurrency_limit"`
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
	} else {
		parsedRS.Bucket = rs.Bucket
	}
	parsedRS.IsLocalBucket = rs.IsLocalBucket

	if parsedInput, err := parseInputFormat(rs.IntputFormat); err != nil {
		return nil, err
	} else {
		parsedRS.InputFormat = parsedInput
	}
	parsedRS.IgnoreMissingFiles = rs.IgnoreMissingFiles

	if !validateExtension(rs.Extension) {
		return nil, errInvalidExtension
	} else {
		parsedRS.Extension = rs.Extension
	}

	if rs.OutputShardSize <= 0 {
		return nil, errNegOutputShardSize
	} else {
		parsedRS.OutputShardSize = rs.OutputShardSize
	}
	if parsedOutput, err := parseOutputFormat(rs.OutputFormat); err != nil {
		return nil, err
	} else {
		parsedRS.OutputFormat = parsedOutput
	}

	if parsedAlgorithm, err := parseAlgorithm(rs.Algorithm); err != nil {
		return nil, errInvalidAlgorithm
	} else {
		parsedRS.Algorithm = parsedAlgorithm
	}

	if rs.MaxMemUsage == "" {
		rs.MaxMemUsage = "80%" // default value
	}
	if parsedMemUsage, err := parseMemUsage(rs.MaxMemUsage); err != nil {
		return nil, err
	} else {
		parsedRS.MaxMemUsage = parsedMemUsage
	}

	if rs.ExtractConcLimit < 0 {
		return nil, errNegativeConcurrencyLimit
	}
	if rs.CreateConcLimit < 0 {
		return nil, errNegativeConcurrencyLimit
	}

	if rs.ExtractConcLimit == 0 {
		rs.ExtractConcLimit = defaultConcLimit
	}
	if rs.CreateConcLimit == 0 {
		rs.CreateConcLimit = defaultConcLimit
	}
	parsedRS.ExtractConcLimit = rs.ExtractConcLimit
	parsedRS.CreateConcLimit = rs.CreateConcLimit
	return parsedRS, nil
}

// validateExtension checks if extension is supported by dsort
func validateExtension(ext string) bool {
	return cmn.StringInSlice(ext, supportedExtensions)
}

func parseBashTemplate(template string) (prefix, suffix string, start, end, step, digitCount int, err error) {
	// "prefix-{00001..00010..2}-suffix"
	left := strings.Index(template, "{")
	if left == -1 {
		return "", "", 0, 0, 0, 0, errInvalidBashFormat
	}
	right := strings.Index(template, "}")
	if right == -1 {
		return "", "", 0, 0, 0, 0, errInvalidBashFormat
	}
	if right < left {
		return "", "", 0, 0, 0, 0, errInvalidBashFormat
	}
	prefix = template[:left]
	if len(template) > right+1 {
		suffix = template[right+1:]
	}
	inside := template[left+1 : right]
	numbers := strings.Split(inside, "..")
	if len(numbers) < 2 || len(numbers) > 3 {
		return "", "", 0, 0, 0, 0, errInvalidBashFormat
	} else if len(numbers) == 2 { // {0001..0999} case
		if start, err = strconv.Atoi(numbers[0]); err != nil {
			return "", "", 0, 0, 0, 0, err
		}
		if end, err = strconv.Atoi(numbers[1]); err != nil {
			return "", "", 0, 0, 0, 0, err
		}
		step = 1
		digitCount = cmn.Min(len(numbers[0]), len(numbers[1]))
	} else if len(numbers) == 3 { // {0001..0999..2} case
		if start, err = strconv.Atoi(numbers[0]); err != nil {
			return "", "", 0, 0, 0, 0, err
		}
		if end, err = strconv.Atoi(numbers[1]); err != nil {
			return "", "", 0, 0, 0, 0, err
		}
		if step, err = strconv.Atoi(numbers[2]); err != nil {
			return "", "", 0, 0, 0, 0, err
		}
		digitCount = cmn.Min(len(numbers[0]), len(numbers[1]))
	}
	return
}

func parseAtTemplate(template string) (prefix, suffix string, start, end, step, digitCount int, err error) {
	// "prefix-@00001-suffix"
	left := strings.Index(template, "@")
	if left == -1 {
		return "", "", 0, 0, 0, 0, errInvalidAtFormat
	}
	prefix = template[:left]
	number := ""
	for left++; len(template) > left && unicode.IsDigit(rune(template[left])); left++ {
		number += string(template[left])
	}

	if len(template) > left {
		suffix = template[left:]
	}

	start = 0
	if end, err = strconv.Atoi(number); err != nil {
		return "", "", 0, 0, 0, 0, err
	}
	step = 1
	digitCount = len(number)
	return
}

// parseInputFormat checks if input format was specified correctly
func parseInputFormat(inputFormat string) (pit *parsedInputTemplate, err error) {
	template := strings.TrimSpace(inputFormat)
	pt := &parsedInputTemplate{}
	if pt.Prefix, pt.Suffix, pt.Start, pt.End, pt.Step, pt.DigitCount, err = parseBashTemplate(template); err == nil {
		pt.Type = templBash
	} else if pt.Prefix, pt.Suffix, pt.Start, pt.End, pt.Step, pt.DigitCount, err = parseAtTemplate(template); err == nil {
		pt.Type = templAt
	} else {
		return nil, errInvalidInputFormat
	}

	if pt.Start > pt.End {
		return nil, errStartAfterEnd
	}
	if pt.Start < 0 {
		return nil, errNegativeStart
	}
	if pt.Step <= 0 {
		return nil, errNonPositiveStep
	}

	pt.RangeCount = ((pt.End - pt.Start) / pt.Step) + 1
	return pt, nil
}

// parseOutputFormat checks if output format was specified correctly
func parseOutputFormat(outputFormat string) (pot *parsedOutputTemplate, err error) {
	template := strings.TrimSpace(outputFormat)
	pt := &parsedOutputTemplate{}
	if pt.Prefix, pt.Suffix, pt.Start, pt.End, pt.Step, pt.DigitCount, err = parseBashTemplate(template); err == nil {
		// Pass
	} else if pt.Prefix, pt.Suffix, pt.Start, pt.End, pt.Step, pt.DigitCount, err = parseAtTemplate(template); err == nil {
		// Pass
	} else {
		return nil, errInvalidOutputFormat
	}

	if pt.Start > pt.End {
		return nil, errStartAfterEnd
	}
	if pt.Start < 0 {
		return nil, errNegativeStart
	}
	if pt.Step <= 0 {
		return nil, errNonPositiveStep
	}

	pt.RangeCount = ((pt.End - pt.Start) / pt.Step) + 1
	return pt, nil
}

func parseMemUsage(memUsage string) (*parsedMemUsage, error) {
	memUsage = strings.Replace(memUsage, " ", "", -1)
	idx := 0
	number := ""
	for ; idx < len(memUsage) && unicode.IsDigit(rune(memUsage[idx])); idx++ {
		number += string(memUsage[idx])
	}

	parsedMU := &parsedMemUsage{}
	if value, err := strconv.Atoi(number); err != nil {
		return nil, errInvalidMemUsage
	} else if value < 0 {
		return nil, errInvalidMemUsage
	} else {
		parsedMU.Value = uint64(value)
	}

	if len(memUsage) <= idx {
		return nil, errInvalidMemUsage
	}

	suffix := memUsage[idx:]
	if suffix == "%" {
		parsedMU.Type = memPercent
		if parsedMU.Value <= 0 || parsedMU.Value >= 100 {
			return nil, errInvalidMaxMemPercent
		}
	} else if value, err := cmn.S2B(memUsage); err != nil {
		return nil, err
	} else if value < 0 {
		return nil, errInvalidMaxMemNumber
	} else {
		parsedMU.Type = memNumber
		parsedMU.Value = uint64(value)
	}

	return parsedMU, nil
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
