// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"sort"

	"github.com/NVIDIA/aistore/cmn"
)

func sortBckEntries(bckEntries []*cmn.BucketEntry) {
	entryLess := func(i, j int) bool {
		if bckEntries[i].Name == bckEntries[j].Name {
			return bckEntries[i].Flags&cmn.EntryStatusMask < bckEntries[j].Flags&cmn.EntryStatusMask
		}
		return bckEntries[i].Name < bckEntries[j].Name
	}
	sort.Slice(bckEntries, entryLess)
}

func deduplicateBckEntries(bckEntries []*cmn.BucketEntry, maxSize uint) ([]*cmn.BucketEntry, string) {
	objCount := uint(len(bckEntries))

	j := 0
	pageMarker := ""
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
	if maxSize > 0 && objCount > maxSize {
		pageMarker = bckEntries[j-1].Name
	}
	return bckEntries[:j], pageMarker
}

// ConcatObjLists takes a slice of object lists and concatenates them: all lists
// are appended to the first one.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func ConcatObjLists(lists []*cmn.BucketList, maxSize uint) (objs *cmn.BucketList) {
	if len(lists) == 0 {
		return &cmn.BucketList{}
	}

	objs = &cmn.BucketList{}
	objs.Entries = make([]*cmn.BucketEntry, 0)

	for _, l := range lists {
		objs.Entries = append(objs.Entries, l.Entries...)
	}

	if len(objs.Entries) == 0 {
		return objs
	}

	// For corner case: we have objects with replicas on page threshold
	// we have to sort taking status into account. Otherwise wrong
	// one(Status=moved) may get into the response
	sortBckEntries(objs.Entries)

	// Remove duplicates
	objs.Entries, objs.PageMarker = deduplicateBckEntries(objs.Entries, maxSize)

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
func MergeObjLists(lists []*cmn.BucketList, maxSize uint) (objs *cmn.BucketList) {
	if len(lists) == 0 {
		return &cmn.BucketList{}
	}

	bckList := lists[0] // main list to collect all info
	pageMarker := bckList.PageMarker

	if len(lists) == 1 {
		sortBckEntries(bckList.Entries)
		bckList.Entries, _ = deduplicateBckEntries(bckList.Entries, maxSize)
		bckList.PageMarker = pageMarker
		return bckList
	}

	objSet := make(map[string]*cmn.BucketEntry, len(bckList.Entries))
	for _, l := range lists {
		if pageMarker < l.PageMarker {
			pageMarker = l.PageMarker
		}
		for _, e := range l.Entries {
			entry, exists := objSet[e.Name]
			if !exists {
				objSet[e.Name] = e
				continue
			}
			// detect which list contains real information about the object
			if !entry.CheckExists() && e.CheckExists() {
				e.Version = cmn.Either(e.Version, entry.Version)
				objSet[e.Name] = e
			} else {
				// TargetURL maybe filled even if an object is not cached
				entry.TargetURL = cmn.Either(entry.TargetURL, e.TargetURL)
				entry.Version = cmn.Either(entry.Version, e.Version)
			}
		}
	}

	if len(objSet) == 0 {
		return objs
	}

	// cleanup and refill
	bckList.Entries = bckList.Entries[:0]
	for _, v := range objSet {
		bckList.Entries = append(bckList.Entries, v)
	}

	sortBckEntries(bckList.Entries)
	bckList.Entries, _ = deduplicateBckEntries(bckList.Entries, maxSize)
	bckList.PageMarker = pageMarker
	return bckList
}
