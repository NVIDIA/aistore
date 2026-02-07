//go:build dsort

// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"slices"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var _ validator = (*DsortConf)(nil)

func dropDsortConfig(*Config) {}

///////////////
// DsortConf //
///////////////

const (
	IgnoreReaction = "ignore"
	WarnReaction   = "warn"
	AbortReaction  = "abort"
)

const _idsort = "invalid distributed_sort."

var SupportedReactions = []string{IgnoreReaction, WarnReaction, AbortReaction}

func (c *DsortConf) Validate() (err error) {
	if c.SbundleMult < 0 || c.SbundleMult > 16 {
		return fmt.Errorf(_idsort+"bundle_multiplier: %v (expected range [0, 16])", c.SbundleMult)
	}
	if !apc.IsValidCompression(c.Compression) {
		return fmt.Errorf(_idsort+"compression: %q (expecting one of: %v)", c.Compression, apc.SupportedCompression)
	}
	return c.ValidateWithOpts(false)
}

func (c *DsortConf) ValidateWithOpts(allowEmpty bool) (err error) {
	f := func(reaction string) bool {
		return ((allowEmpty && reaction == "") || slices.Contains(SupportedReactions, reaction))
	}

	const s = "expecting one of:"
	if !f(c.DuplicatedRecords) {
		return fmt.Errorf(_idsort+"duplicated_records: %s (%s %v)", c.DuplicatedRecords, s, SupportedReactions)
	}
	if !f(c.MissingShards) {
		return fmt.Errorf(_idsort+"missing_shards: %s (%s %v)", c.MissingShards, s, SupportedReactions)
	}
	if !f(c.EKMMalformedLine) {
		return fmt.Errorf(_idsort+"ekm_malformed_line: %s (%s %v)", c.EKMMalformedLine, s, SupportedReactions)
	}
	if !f(c.EKMMissingKey) {
		return fmt.Errorf(_idsort+"ekm_missing_key: %s (%s %v)", c.EKMMissingKey, s, SupportedReactions)
	}
	if !allowEmpty {
		if _, err := cos.ParseQuantity(c.DefaultMaxMemUsage); err != nil {
			return fmt.Errorf(_idsort+"default_max_mem_usage: %s (err: %v)", c.DefaultMaxMemUsage, err)
		}
	}
	if _, err := cos.ParseSize(c.DsorterMemThreshold, cos.UnitsIEC); err != nil && (!allowEmpty || c.DsorterMemThreshold != "") {
		return fmt.Errorf(_idsort+"dsorter_mem_threshold: %s (err: %v)", c.DsorterMemThreshold, err)
	}
	return nil
}
