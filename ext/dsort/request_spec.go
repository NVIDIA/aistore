//go:build sharding

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
)

/////////////////
// RequestSpec //
/////////////////

func specErr(s string, err error) error { return fmt.Errorf("[dsort] parse-spec: %q %w", s, err) }

func (rs *RequestSpec) ParseCtx() (*ParsedReq, error) {
	pars, err := rs.parse()
	return &ParsedReq{pars.InputBck, pars.OutputBck, pars}, err
}

func (rs *RequestSpec) parse() (*parsedReqSpec, error) {
	var (
		cfg  = cmn.GCO.Get().Dsort
		pars = &parsedReqSpec{}
	)

	// src bck
	if rs.InputBck.IsEmpty() {
		return pars, specErr("input_bck", errMissingSrcBucket)
	}
	pars.InputBck = rs.InputBck
	if rs.InputBck.Provider == "" {
		pars.InputBck.Provider = apc.AIS // NOTE: ais:// is the default
	} else {
		normp, err := cmn.NormalizeProvider(rs.InputBck.Provider)
		if err != nil {
			return pars, specErr("input_bck_provider", err)
		}
		pars.InputBck.Provider = normp
	}
	if err := rs.InputBck.Validate(); err != nil {
		return pars, specErr("input_bck", err)
	}

	pars.Description = rs.Description

	// dst bck
	pars.OutputBck = rs.OutputBck
	if pars.OutputBck.IsEmpty() {
		pars.OutputBck = pars.InputBck // NOTE: source can be the destination as well
	} else {
		normp, err := cmn.NormalizeProvider(rs.OutputBck.Provider)
		if err != nil {
			return pars, specErr("output_bck_provider", err)
		}
		pars.OutputBck.Provider = normp
		if err := rs.OutputBck.Validate(); err != nil {
			return pars, specErr("output_bck", err)
		}
	}

	// input format
	var err error
	pars.Pit, err = parseInputFormat(rs.InputFormat)
	if err != nil {
		return nil, specErr("input_format", err)
	}
	if rs.InputFormat.Template != "" {
		// template is not a filename but all we do here is
		// checking the template's suffix for specific supported extensions
		if ext, err := archive.Mime("", rs.InputFormat.Template); err == nil {
			if rs.InputExtension != "" && rs.InputExtension != ext {
				return nil, fmt.Errorf("input_extension: %q vs %q", rs.InputExtension, ext)
			}
			rs.InputExtension = ext
		}
	}
	if rs.InputExtension != "" {
		pars.InputExtension, err = archive.Mime(rs.InputExtension, "")
		if err != nil {
			return nil, specErr("input_extension", err)
		}
	}

	// output format
	pars.OutputShardSize, err = cos.ParseSize(rs.OutputShardSize, cos.UnitsIEC)
	if err != nil {
		return nil, specErr("output_shard_size", err)
	}
	if pars.OutputShardSize < 0 {
		return nil, fmt.Errorf(fmtErrNegOutputSize, pars.OutputShardSize)
	}
	pars.Algorithm, err = parseAlgorithm(rs.Algorithm)
	if err != nil {
		return nil, specErr("algorithm", err)
	}

	var isEKM bool
	if isEKM, err = validateEKMFileURL(rs.EKMFileURL); err != nil {
		return nil, fmt.Errorf(fmtErrOrderURL, rs.EKMFileURL, err)
	}
	if isEKM {
		if pars.Pot, err = parseOutputFormat(rs.OutputFormat); err != nil {
			return nil, err
		}
		if pars.Pot.Template.Count() > math.MaxInt32 {
			// If the count is not defined the output shard size must be
			if pars.OutputShardSize == 0 {
				return nil, errMissingOutputSize
			}
		}
		if rs.OutputFormat != "" {
			// (ditto)
			if ext, err := archive.Mime("", rs.OutputFormat); err == nil {
				if rs.OutputExtension != "" && rs.OutputExtension != ext {
					return nil, fmt.Errorf("output_extension: %q vs %q", rs.OutputExtension, ext)
				}
				rs.OutputExtension = ext
			}
		}
	} else {
		// If the ekm file is provided, the output shard size must be set.
		if pars.OutputShardSize == 0 {
			return nil, errMissingOutputSize
		}
		pars.EKMFileURL = rs.EKMFileURL
		pars.EKMFileSep = rs.EKMFileSep
		if pars.EKMFileSep == "" {
			pars.EKMFileSep = "\t"
		}
	}
	if rs.OutputExtension == "" {
		pars.OutputExtension = pars.InputExtension // default
	} else {
		pars.OutputExtension, err = archive.Mime(rs.OutputExtension, "")
		if err != nil {
			return nil, specErr("output_extension", err)
		}
	}

	// mem & conc
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
	pars.DsorterType = rs.DsorterType
	pars.DryRun = rs.DryRun

	// `cfg` here contains inherited (aka global) part of the dsort config -
	// apply this request's rs.Config values to override or assign defaults

	if err := rs.Config.ValidateWithOpts(true); err != nil {
		return nil, err
	}
	pars.DsortConf = rs.Config

	pars.SbundleMult = rs.Config.SbundleMult
	if pars.SbundleMult == 0 {
		pars.SbundleMult = cfg.SbundleMult
	}
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
	if pars.DsorterMemThreshold == "" {
		pars.DsorterMemThreshold = cfg.DsorterMemThreshold
	}

	return pars, nil
}

func parseAlgorithm(alg Algorithm) (*Algorithm, error) {
	if !slices.Contains(algorithms, alg.Kind) {
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
		if err := shard.ValidateContentKeyTy(alg.ContentKeyType); err != nil {
			return nil, err
		}
	} else {
		alg.ContentKeyType = shard.ContentKeyString
	}

	return &alg, nil
}

func validateEKMFileURL(ekmURL string) (empty bool, err error) {
	if ekmURL == "" {
		return true, nil
	}
	_, err = url.ParseRequestURI(ekmURL)
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
	if err := pot.Template.CheckIsRange(); err != nil {
		return nil, err
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
		pit.Prefix = cos.EmptyMatchAll
	} else if err == nil && pit.Template.IsPrefixOnly() {
		// prefix only
		pit.Prefix = pit.Template.Prefix
	}
	return
}

func (pit *parsedInputTemplate) isList() bool   { return len(pit.ObjNames) > 0 }
func (pit *parsedInputTemplate) isPrefix() bool { return !pit.isList() && !pit.Template.IsRange() }
