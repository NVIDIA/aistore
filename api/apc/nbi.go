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

		// PagesPerChunk overrides the default number of pages to pack into a single inventory chunk.
		// If zero, the default is used.
		PagesPerChunk int64 `json:"pages_per_chunk,omitempty"`

		// MaxEntriesPerChunk puts a hard cap on the number of entries in a single inventory chunk.
		// If zero, the cap is disabled.
		MaxEntriesPerChunk int64 `json:"max_entries_per_chunk,omitempty"`

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

//////////////////
// CreateNBIMsg //
//////////////////

const (
	DefaultInvPagesPerChunk = 50
	MaxInvPagesPerChunk     = 256
	MaxInvEntriesPerChunk   = MaxInvPagesPerChunk * MaxPageSizeGlobal
)

// validate; set defaults
func (m *CreateNBIMsg) SetValidate() error {
	const etag = "invalid '" + ActCreateNBI + "'"

	// 1) disallow
	if m.ContinuationToken != "" {
		return errors.New(etag + ": continuation_token must be empty")
	}
	if m.StartAfter != "" {
		return errors.New(etag + ": start_after is not supported")
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
		return fmt.Errorf("%s: %s", etag, sb.String())
	}

	// 2) advanced tunables
	switch {
	case m.PagesPerChunk == 0:
		m.PagesPerChunk = DefaultInvPagesPerChunk
	case m.PagesPerChunk < 0:
		return fmt.Errorf("%s: pages_per_chunk=%d", etag, m.PagesPerChunk)
	case m.PagesPerChunk > MaxInvPagesPerChunk:
		return fmt.Errorf("%s: pages_per_chunk too large: %d", etag, m.PagesPerChunk)
	}
	if m.MaxEntriesPerChunk < 0 {
		return fmt.Errorf("%s: negative max_entries_per_chunk=%d", etag, m.MaxEntriesPerChunk)
	}
	if m.MaxEntriesPerChunk > MaxInvEntriesPerChunk {
		return fmt.Errorf("%s: too large max_entries_per_chunk=%d", etag, m.MaxEntriesPerChunk)
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
