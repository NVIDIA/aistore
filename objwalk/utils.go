// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"sort"

	"github.com/NVIDIA/aistore/cmn"
)

// ConcatObjLists takes a slice of object lists and concatenates them: all lists
// are appended to the first one.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func ConcatObjLists(lists [][]*cmn.BucketEntry, maxSize int) (objs []*cmn.BucketEntry, marker string) {
	for _, l := range lists {
		if objs == nil {
			objs = l
			continue
		}

		objs = append(objs, l...)
	}

	objCount := len(objs)
	// return all objects
	if maxSize <= 0 || objCount <= maxSize {
		return objs, ""
	}

	// sort the result but only if the set is greater than page size
	// For corner case: we have objects with replicas on page threshold
	// we have to sort taking status into account. Otherwise wrong
	// one(Status=moved) may get into the response
	ifLess := func(i, j int) bool {
		if objs[i].Name == objs[j].Name {
			return objs[i].Flags&cmn.EntryStatusMask < objs[j].Flags&cmn.EntryStatusMask
		}
		return objs[i].Name < objs[j].Name
	}
	sort.Slice(objs, ifLess)
	// set extra infos to nil to avoid memory leaks
	// see NOTE on https://github.com/golang/go/wiki/SliceTricks
	for i := maxSize; i < objCount; i++ {
		objs[i] = nil
	}
	objs = objs[:maxSize]

	return objs, objs[maxSize-1].Name
}

// MergeObjLists takes a few object lists and merges its content: properties
// of objects with the same name are merged. It results in the first list
// contains the most full information about objects.
// The function is used, e.g, to merge a few requests to targets for a cloud
// bucket list: each target reads cloud list page and fills with available info.
// Then the proxy receives these lists, that contains the same objects but with
// different object filled, and makes one list with full info.
// If maxSize is greater than 0, the resulting list is sorted and truncated. Zero
// or negative maxSize means returning all objects.
func MergeObjLists(lists []*cmn.BucketList, maxSize int) (objs *cmn.BucketList, marker string) {
	if len(lists) == 1 {
		return lists[0], lists[0].PageMarker
	}

	bckList := lists[0] // main list to collect all info
	objSet := make(map[string]*cmn.BucketEntry, len(bckList.Entries))
	pageMarker := bckList.PageMarker
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

	// cleanup and refill
	bckList.Entries = bckList.Entries[:0]
	for _, v := range objSet {
		bckList.Entries = append(bckList.Entries, v)
	}

	// sort the result
	ifLess := func(i, j int) bool {
		if bckList.Entries[i].Name == bckList.Entries[j].Name {
			return bckList.Entries[i].Flags&cmn.EntryStatusMask < bckList.Entries[j].Flags&cmn.EntryStatusMask
		}
		return bckList.Entries[i].Name < bckList.Entries[j].Name
	}
	sort.Slice(bckList.Entries, ifLess)

	objCount := len(bckList.Entries)
	if maxSize <= 0 || objCount <= maxSize {
		return bckList, pageMarker
	}

	// set extra infos to nil to avoid memory leaks
	// see NOTE on https://github.com/golang/go/wiki/SliceTricks
	for i := maxSize; i < objCount; i++ {
		bckList.Entries[i] = nil
	}
	bckList.Entries = bckList.Entries[:maxSize]
	pageMarker = bckList.Entries[maxSize-1].Name

	return bckList, pageMarker
}
