// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

func SortBckEntries(bckEntries []*ObjEntry) {
	entryLess := func(i, j int) bool {
		if bckEntries[i].Name == bckEntries[j].Name {
			return bckEntries[i].Flags&apc.EntryStatusMask < bckEntries[j].Flags&apc.EntryStatusMask
		}
		return bckEntries[i].Name < bckEntries[j].Name
	}
	sort.Slice(bckEntries, entryLess)
}

func deduplicateBckEntries(bckEntries []*ObjEntry, maxSize uint) ([]*ObjEntry, string) {
	objCount := uint(len(bckEntries))

	j := 0
	token := ""
	for _, obj := range bckEntries {
		if j > 0 && bckEntries[j-1].Name == obj.Name {
			continue
		}
		bckEntries[j] = obj
		j++

		if maxSize > 0 && j == int(maxSize) {
			break
		}
	}

	// Set extra infos to nil to avoid memory leaks
	// see NOTE on https://github.com/golang/go/wiki/SliceTricks
	for i := j; i < int(objCount); i++ {
		bckEntries[i] = nil
	}
	if maxSize > 0 && objCount >= maxSize {
		token = bckEntries[j-1].Name
	}
	return bckEntries[:j], token
}

// ConcatObjLists takes a slice of object lists and concatenates them: all lists
// are appended to the first one.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func ConcatObjLists(lists []*ListObjects, maxSize uint) (objs *ListObjects) {
	if len(lists) == 0 {
		return &ListObjects{}
	}

	objs = &ListObjects{}
	objs.Entries = make([]*ObjEntry, 0)

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
	SortBckEntries(objs.Entries)

	// Remove duplicates
	objs.Entries, objs.ContinuationToken = deduplicateBckEntries(objs.Entries, maxSize)
	return
}

// MergeObjLists takes a few object lists and merges its content: properties
// of objects with the same name are merged.
// The function is used to merge eg. the requests from targets for a cloud
// bucket list: each target reads cloud list page and fills with available info.
// Then the proxy receives these lists that contains the same objects and merges
// them to get single list with merged information for each object.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func MergeObjLists(lists []*ListObjects, maxSize uint) (objs *ListObjects) {
	if len(lists) == 0 {
		return &ListObjects{}
	}

	bckList := lists[0] // main list to collect all info
	contiunationToken := bckList.ContinuationToken

	if len(lists) == 1 {
		SortBckEntries(bckList.Entries)
		bckList.Entries, _ = deduplicateBckEntries(bckList.Entries, maxSize)
		bckList.ContinuationToken = contiunationToken
		return bckList
	}

	objSet := make(map[string]*ObjEntry, len(bckList.Entries))
	for _, l := range lists {
		bckList.Flags |= l.Flags
		if contiunationToken < l.ContinuationToken {
			contiunationToken = l.ContinuationToken
		}
		for _, e := range l.Entries {
			entry, exists := objSet[e.Name]
			if !exists {
				objSet[e.Name] = e
				continue
			}
			// detect which list contains real information about the object
			if !entry.CheckExists() && e.CheckExists() {
				e.Version = cos.Either(e.Version, entry.Version)
				objSet[e.Name] = e
			} else {
				// TargetURL maybe filled even if an object is not cached
				entry.TargetURL = cos.Either(entry.TargetURL, e.TargetURL)
				entry.Version = cos.Either(entry.Version, e.Version)
			}
		}
	}

	if len(objSet) == 0 {
		return &ListObjects{}
	}

	// cleanup and refill
	bckList.Entries = bckList.Entries[:0]
	for _, v := range objSet {
		bckList.Entries = append(bckList.Entries, v)
	}

	SortBckEntries(bckList.Entries)
	bckList.Entries, _ = deduplicateBckEntries(bckList.Entries, maxSize)
	bckList.ContinuationToken = contiunationToken
	return bckList
}

// Returns true if the (continuation) token includes the object's name
// (in other words, the object is already listed and must be skipped)
func TokenIncludesObject(token, objName string) bool {
	return strings.Compare(token, objName) >= 0
}

func DirNameContainsPrefix(dirPath, prefix string) bool {
	// Every directory has to either:
	// - be contained in prefix (for levels lower than prefix: prefix="abcd/def", directory="abcd")
	// - include prefix (for levels deeper than prefix: prefix="a/", directory="a/b")
	return prefix == "" || (strings.HasPrefix(prefix, dirPath) || strings.HasPrefix(dirPath, prefix))
}

func ObjNameContainsPrefix(objName, prefix string) bool {
	return prefix == "" || strings.HasPrefix(objName, prefix)
}
