// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type LsoEntries []*LsoEntry // separately from (code-generated) objlist* - no need to msgpack

func SortLso(bckEntries LsoEntries) {
	entryLess := func(i, j int) bool {
		if bckEntries[i].Name == bckEntries[j].Name {
			return bckEntries[i].Flags&apc.EntryStatusMask < bckEntries[j].Flags&apc.EntryStatusMask
		}
		return bckEntries[i].Name < bckEntries[j].Name
	}
	sort.Slice(bckEntries, entryLess)
}

func dedupLso(entries LsoEntries, maxSize uint) ([]*LsoEntry, string) {
	var (
		token    string
		j        int
		objCount = uint(len(entries))
	)
	for _, obj := range entries {
		if j > 0 && entries[j-1].Name == obj.Name {
			continue
		}
		entries[j] = obj
		j++

		if maxSize > 0 && j == int(maxSize) {
			break
		}
	}
	// nullify discarded entries to avoid leaks (e.g. https://github.com/golang/go/wiki/SliceTricks)
	for i := j; i < int(objCount); i++ {
		entries[i] = nil
	}
	if maxSize > 0 && objCount >= maxSize {
		token = entries[j-1].Name
	}
	return entries[:j], token
}

// ConcatLso takes a slice of object lists and concatenates them: all lists
// are appended to the first one.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func ConcatLso(lists []*LsoResult, maxSize uint) (objs *LsoResult) {
	if len(lists) == 0 {
		return &LsoResult{}
	}

	objs = &LsoResult{}
	objs.Entries = make(LsoEntries, 0)

	for _, l := range lists {
		objs.Flags |= l.Flags
		objs.Entries = append(objs.Entries, l.Entries...)
	}

	if len(objs.Entries) == 0 {
		return objs
	}

	// For corner case: we have objects with replicas on page threshold
	// we have to sort taking status into account. Otherwise wrong
	// one(Status=moved) may get into the response
	SortLso(objs.Entries)

	// Remove duplicates
	objs.Entries, objs.ContinuationToken = dedupLso(objs.Entries, maxSize)
	return
}

// MergeLso takes a few object lists and merges its content: properties
// of objects with the same name are merged.
// The function is used to merge eg. the requests from targets for a cloud
// bucket list: each target reads cloud list page and fills with available info.
// Then the proxy receives these lists that contains the same objects and merges
// them to get single list with merged information for each object.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func MergeLso(lists []*LsoResult, maxSize uint) *LsoResult {
	if len(lists) == 0 {
		return &LsoResult{}
	}
	resList := lists[0]
	continuationToken := resList.ContinuationToken
	if len(lists) == 1 {
		SortLso(resList.Entries)
		resList.Entries, _ = dedupLso(resList.Entries, maxSize)
		resList.ContinuationToken = continuationToken
		return resList
	}

	lst := make(map[string]*LsoEntry, len(resList.Entries))
	for _, l := range lists {
		resList.Flags |= l.Flags
		if continuationToken < l.ContinuationToken {
			continuationToken = l.ContinuationToken
		}
		for _, e := range l.Entries {
			entry, exists := lst[e.Name]
			if !exists {
				lst[e.Name] = e
				continue
			}
			// detect which list contains real information about the object
			if !entry.CheckExists() && e.CheckExists() {
				e.Version = cos.Either(e.Version, entry.Version)
				lst[e.Name] = e
			} else {
				entry.Location = cos.Either(entry.Location, e.Location)
				entry.Version = cos.Either(entry.Version, e.Version)
			}
		}
	}

	if len(lst) == 0 {
		return &LsoResult{}
	}

	// cleanup and sort
	resList.Entries = resList.Entries[:0]
	for _, v := range lst {
		resList.Entries = append(resList.Entries, v)
	}
	SortLso(resList.Entries)
	resList.Entries, _ = dedupLso(resList.Entries, maxSize)
	resList.ContinuationToken = continuationToken
	return resList
}

// Returns true if the continuation token >= object's name (in other words, the object is
// already listed and must be skipped). Note that string `>=` is lexicographic.
func TokenGreaterEQ(token, objName string) bool { return token >= objName }

// Every directory has to either:
// - be contained in prefix (for levels lower than prefix: prefix="abcd/def", directory="abcd")
// - include prefix (for levels deeper than prefix: prefix="a/", directory="a/b")
func DirNameContainsPrefix(dirPath, prefix string) bool {
	return prefix == "" || (strings.HasPrefix(prefix, dirPath) || strings.HasPrefix(dirPath, prefix))
}

func ObjNameContainsPrefix(objName, prefix string) bool {
	return prefix == "" || strings.HasPrefix(objName, prefix)
}
