// Package config provides types and functions to configure ishard executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package config

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
)

type SampleKeyPattern struct {
	Regex        string
	CaptureGroup string
}

// Define some commonly used sample key patterns
var (
	BaseFileNamePattern   = SampleKeyPattern{Regex: `.*/([^/]+)$`, CaptureGroup: "$1"}
	FullNamePattern       = SampleKeyPattern{Regex: `^(.*)$`, CaptureGroup: "$1"}
	CollapseAllDirPattern = SampleKeyPattern{Regex: `/`, CaptureGroup: ""}
)

// MissingExtManager contains the set of expected extensions for each sample, and corresponding reaction
type MissingExtManager struct {
	Name             string
	EffectiveObjSize int64
	extSet           cos.StrSet

	// Action to take on the given Records, returns the potentially updated Records and any error encountered
	React func(*shard.Records) (*shard.Records, error)
}

func NewMissingExtManager(name string, sampleExts []string) (*MissingExtManager, error) {
	if len(sampleExts) == 0 {
		return nil, fmt.Errorf("invalid extensions, should have at least one specified extension")
	}
	for _, ext := range sampleExts {
		if ext == "" {
			return nil, fmt.Errorf("invalid extensions, extension can't be empty string \"\"")
		}
	}
	mgr := &MissingExtManager{
		Name:   name,
		extSet: cos.NewStrSet(sampleExts...),
	}

	switch name {
	case "ignore":
		mgr.React = mgr.ignore
	case "warn":
		mgr.React = mgr.warn
	case "abort":
		mgr.React = mgr.abort
	case "exclude":
		mgr.React = mgr.exclude
	default:
		debug.Assert(false)
		return nil, fmt.Errorf("invalid action: %s. Accepted values are: abort, warn, ignore, exclude", name)
	}

	return mgr, nil
}

func (mgr *MissingExtManager) ignore(recs *shard.Records) (*shard.Records, error) {
	for _, record := range recs.All() {
		mgr.EffectiveObjSize += record.TotalSize()
	}
	return nil, nil
}

func (mgr *MissingExtManager) warn(recs *shard.Records) (*shard.Records, error) {
	for _, record := range recs.All() {
		mgr.EffectiveObjSize += record.TotalSize()
		extra, missing := difference(mgr.extSet, record.Objects)
		for ext := range extra {
			fmt.Printf("[Warning] sample %s contains extension %s, not specified in `sample_ext` config\n", record.Name, ext)
		}
		for ext := range missing {
			fmt.Printf("[Warning] extension %s not found in sample %s\n", ext, record.Name)
		}
	}

	return nil, nil
}

func (mgr *MissingExtManager) abort(recs *shard.Records) (*shard.Records, error) {
	for _, record := range recs.All() {
		mgr.EffectiveObjSize += record.TotalSize()
		extra, missing := difference(mgr.extSet, record.Objects)
		for ext := range extra {
			return nil, fmt.Errorf("sample %s contains extension %s, not specified in `sample_ext` config", record.Name, ext)
		}
		for ext := range missing {
			return nil, fmt.Errorf("missing extension: extension %s not found in sample %s", ext, record.Name)
		}
	}

	return nil, nil
}

func (mgr *MissingExtManager) exclude(recs *shard.Records) (*shard.Records, error) {
	filteredRecs := shard.NewRecords(16)

	for _, record := range recs.All() {
		extra, missing := difference(mgr.extSet, record.Objects)
		for ext := range extra {
			recs.DeleteDup(record.Name, ext)
		}
		if len(missing) == 0 {
			filteredRecs.Insert(record)
			mgr.EffectiveObjSize += record.TotalSize()
		}
	}

	return filteredRecs, nil
}

// difference finds the differences between two sets: `want` and `have`.
// returns `extra` (extensions in `have` but not in `want`) and `missing` (extensions in `want` but not in `have`).
func difference(want cos.StrSet, have []*shard.RecordObj) (extra cos.StrSet, missing cos.StrSet) {
	missing = want.Clone()
	extra = cos.NewStrSet()
	for _, obj := range have {
		if !missing.Contains(obj.Extension) {
			extra.Add(obj.Extension)
		}
		missing.Delete(obj.Extension)
	}
	return
}
