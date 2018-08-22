/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"fmt"
	"hash"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

const (
	sortKindEmpty   = ""     // default one (sort decreasing)
	SortKindNone    = "none" // none, used for resharding
	SortKindMD5     = "md5"
	SortKindShuffle = "shuffle" // shuffle randomly, can be used with seed to get reproducible results
)

var (
	supportedAlgorithms = []string{sortKindEmpty, SortKindMD5, SortKindShuffle, SortKindNone}
)

type (
	alphaByKey struct {
		*records
		decreasing bool
	}
	keyFunc func(string, hash.Hash) string
)

var (
	_ = keyFunc(keyIdentity)
	_ = keyFunc(keyMD5)

	_ sort.Interface = alphaByKey{}
)

func (s alphaByKey) Len() int      { return len(s.arr) }
func (s alphaByKey) Swap(i, j int) { s.arr[i], s.arr[j] = s.arr[j], s.arr[i] }
func (s alphaByKey) Less(i, j int) bool {
	if s.decreasing {
		return s.arr[i].Key > s.arr[j].Key
	}
	return s.arr[i].Key < s.arr[j].Key
}

// Sort sorts records by each Record.Key in the order determined by sort algorithm.
func (r *records) Sort(algo *SortAlgorithm) {
	if algo.Kind == SortKindNone {
		return
	} else if algo.Kind == SortKindShuffle {
		seed := time.Now().Unix()
		if algo.Seed != "" {
			// We can safely ignore error since we know that the seed was validated
			// during request spec validation.
			seed, _ = strconv.ParseInt(algo.Seed, 10, 64)
		}

		rand.Seed(seed)
		for i := range r.arr { // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
			j := rand.Intn(i + 1)
			r.arr[i], r.arr[j] = r.arr[j], r.arr[i]
		}
	} else {
		sort.Sort(alphaByKey{r, algo.Decreasing})
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
