// Package apc: API control messages and constants
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// native bucket inventory

type (
	CreateNBIMsg struct {
		Name string `json:"name,omitempty"` // inventory name (optional; must be unique for a given bucket)
		LsoMsg

		// Number of object names to store in each inventory chunk.
		// Requested properties are stored alongside each name.
		// Advanced usage only - non-zero overrides system default.
		NamesPerChunk int64 `json:"names_per_chunk,omitempty"`

		// Remove all existing inventories, if any, and proceed to create the new one
		// (only one inventory per bucket is supported).
		Force bool `json:"force,omitempty"`
	}

	NBIInfo struct {
		Bucket   string `json:"bucket"`
		Name     string `json:"name"`
		ObjName  string `json:"obj_name"`
		Prefix   string `json:"prefix,omitempty"`
		Size     int64  `json:"size"`
		Started  int64  `json:"started,omitempty"`
		Finished int64  `json:"finished,omitempty"`
	}
	NBIInfoMap map[string]*NBIInfo // by NBIInfo.Name
)

//
// LsoMsg - NBI extension
//

func (m *LsoMsg) ValidateNBI() error {
	const epref = "invalid list via native bucket inventory"

	// inventory snapshot is flat; StartAfter currently unsupported
	if m.StartAfter != "" {
		return errors.New(epref + ": start_after is not supported")
	}

	// flags that do not make sense for inventory listing
	const badFlags = LsNotCached | LsMissing | LsDeleted | LsArchDir |
		lsWantOnlyRemoteProps | LsNoRecursion | LsDiff

	if m.Flags&badFlags != 0 {
		var sb cos.SB
		sb.Grow(96)
		sb.WriteString("flags:")
		m.appendFlags(&sb)
		return fmt.Errorf("%s: %s", epref, sb.String())
	}

	return nil
}

//////////////////
// CreateNBIMsg //
//////////////////

const (
	DfltInvNamesPerChunk = 2 * MaxPageSizeAIS  // 20K
	MaxInvNamesPerChunk  = 64 * MaxPageSizeAIS // 640K
	MinInvNamesPerChunk  = 2
)

// validate; set defaults
func (m *CreateNBIMsg) SetValidate() error {
	const epref = "invalid '" + ActCreateNBI + "'"

	// 1) disallow
	if m.ContinuationToken != "" {
		return errors.New(epref + ": continuation_token must be empty")
	}
	if m.StartAfter != "" {
		return errors.New(epref + ": start_after is not supported")
	}
	// flags that don't make sense for inventory generation
	const badFlags = LsCached | LsNotCached | LsMissing | LsDeleted | LsArchDir |
		LsBckPresent | LsDontHeadRemote | LsDontAddRemote |
		lsWantOnlyRemoteProps | LsNoRecursion | LsDiff | LsIsS3
	if m.Flags&badFlags != 0 {
		var sb cos.SB
		sb.Grow(96)
		sb.WriteString("flags:")
		m.appendFlags(&sb)
		return fmt.Errorf("%s: %s", epref, sb.String())
	}

	// 2) advanced tunables
	switch {
	case m.NamesPerChunk == 0:
		m.NamesPerChunk = DfltInvNamesPerChunk
	case m.NamesPerChunk < 0:
		return fmt.Errorf("%s: negative names_per_chunk=%d", epref, m.NamesPerChunk)
	case m.NamesPerChunk < MinInvNamesPerChunk:
		return fmt.Errorf("%s: names_per_chunk=%d too small (min=%d)", epref, m.NamesPerChunk, MinInvNamesPerChunk)
	case m.NamesPerChunk > MaxInvNamesPerChunk:
		return fmt.Errorf("%s: names_per_chunk=%d too large (max=%d)", epref, m.NamesPerChunk, MaxInvNamesPerChunk)
	}

	// 3) NOTE: otherwise, backend _may_ append extra (virt-dir) entries (in re: pre-allocation+reuse)
	m.SetFlag(LsNoDirs)

	// 4) absolute minimum
	if m.IsFlagSet(LsNameOnly) {
		m.Props = GetPropsName
		return nil
	}
	if m.IsFlagSet(LsNameSize) {
		if m.Props == "" {
			m.Props = GetPropsNameSize
		} else {
			m.AddProps(GetPropsName, GetPropsSize)
		}
		return nil
	}

	// 5) default props
	if m.Props == "" {
		m.AddProps(GetPropsName, GetPropsSize, GetPropsCached)
	} else {
		m.AddProps(GetPropsCached)
	}

	return nil
}

////////////////
// NBIInfoMap //
////////////////

func (m NBIInfoMap) Names() []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return names
}

func (m NBIInfoMap) SingleName() string {
	for _, info := range m {
		return info.Name
	}
	return ""
}
