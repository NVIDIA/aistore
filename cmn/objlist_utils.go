// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func (entries LsoEntries) less(i, j int) bool {
	if entries[i].Name == entries[j].Name {
		return entries[i].Flags&apc.EntryStatusMask < entries[j].Flags&apc.EntryStatusMask
	}
	return entries[i].Name < entries[j].Name
}

func SortLso(entries LsoEntries) { sort.Slice(entries, entries.less) }

func DedupLso(entries LsoEntries, maxSize int) []*LsoEntry {
	var j int
	for _, obj := range entries {
		if j > 0 && entries[j-1].Name == obj.Name {
			continue
		}
		entries[j] = obj
		j++

		if maxSize > 0 && j == maxSize {
			break
		}
	}
	clear(entries[j:])
	return entries[:j]
}

// MergeLso merges list-objects results received from targets. For the same
// object name (ie., the same object) the corresponding properties are merged.
// If maxSize is greater than 0, the resulting list is sorted and truncated.
func MergeLso(lists []*LsoResult, maxSize int) *LsoResult {
	if len(lists) == 0 {
		return &LsoResult{}
	}
	resList := lists[0]
	continuationToken := resList.ContinuationToken
	if len(lists) == 1 {
		SortLso(resList.Entries)
		resList.Entries = DedupLso(resList.Entries, maxSize)
		resList.ContinuationToken = continuationToken
		return resList
	}

	tmp := make(map[string]*LsoEntry, len(resList.Entries))
	for _, l := range lists {
		resList.Flags |= l.Flags
		if continuationToken < l.ContinuationToken {
			continuationToken = l.ContinuationToken
		}
		for _, e := range l.Entries {
			entry, exists := tmp[e.Name]
			if !exists {
				tmp[e.Name] = e
				continue
			}
			// merge the props
			if !entry.IsPresent() && e.IsPresent() {
				e.Version = cos.Either(e.Version, entry.Version)
				tmp[e.Name] = e
			} else {
				entry.Location = cos.Either(entry.Location, e.Location)
				entry.Version = cos.Either(entry.Version, e.Version)
			}
		}
	}

	// cleanup and sort
	resList.Entries = resList.Entries[:0]
	resList.ContinuationToken = continuationToken

	for _, entry := range tmp {
		resList.Entries = append(resList.Entries, entry)
	}
	SortLso(resList.Entries)
	resList.Entries = DedupLso(resList.Entries, maxSize)

	clear(tmp)
	return resList
}

// Returns true if the continuation token >= object's name (in other words, the object is
// already listed and must be skipped). Note that string `>=` is lexicographic.
func TokenGreaterEQ(token, objName string) bool { return token >= objName }

// Directory has to either:
// - include (or match) prefix, or
// - be contained in prefix - motivation: don't SkipDir a/b when looking for a/b/c
// An alternative name for this function could be smth. like SameBranch()
func DirHasOrIsPrefix(dirPath, prefix string) bool {
	return prefix == "" || (strings.HasPrefix(prefix, dirPath) || strings.HasPrefix(dirPath, prefix))
}

func ObjHasPrefix(objName, prefix string) bool {
	return prefix == "" || strings.HasPrefix(objName, prefix)
}
