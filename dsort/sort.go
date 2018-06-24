/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"fmt"
	"hash"
	"sort"
)

const sortKindMD5 = "md5"

type (
	alphaByKey           []Record
	alphaByKeyDecreasing []Record
	keyFunc              func(string, hash.Hash) string
)

var (
	_ = keyFunc(keyIdentity)
	_ = keyFunc(keyMD5)
)

func (s alphaByKey) Len() int           { return len(s) }
func (s alphaByKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s alphaByKey) Less(i, j int) bool { return s[i].Key < s[j].Key }

func (s alphaByKeyDecreasing) Len() int           { return len(s) }
func (s alphaByKeyDecreasing) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s alphaByKeyDecreasing) Less(i, j int) bool { return s[i].Key > s[j].Key }

// SortRecords sorts records by each Record.Key in the order determined by the boolean `decreasing`.
func SortRecords(records *[]Record, decreasing bool) {
	if decreasing {
		sort.Sort(alphaByKeyDecreasing(*records))
	} else {
		sort.Sort(alphaByKey(*records))
	}
}

func keyIdentity(base string, h hash.Hash) string {
	return base
}

func keyMD5(base string, h hash.Hash) string {
	s := fmt.Sprintf("%x", h.Sum([]byte(base)))
	h.Reset()
	return s
}
