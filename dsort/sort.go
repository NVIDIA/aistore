// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/dsort/extract"
)

const (
	sortKindEmpty   = ""     // default one (sort decreasing)
	SortKindNone    = "none" // none, used for resharding
	SortKindMD5     = "md5"
	SortKindShuffle = "shuffle" // shuffle randomly, can be used with seed to get reproducible results
	SortKindContent = "content" // sort by content of given file
)

var (
	supportedAlgorithms = []string{sortKindEmpty, SortKindMD5, SortKindShuffle, SortKindContent, SortKindNone}
)

type (
	alphaByKey struct {
		*extract.Records
		decreasing bool
		formatType string
	}
)

var (
	_ sort.Interface = alphaByKey{}
)

func (s alphaByKey) Less(i, j int) bool {
	if s.decreasing {
		return s.Records.Less(j, i, s.formatType)
	}
	return s.Records.Less(i, j, s.formatType)
}

// sortRecords sorts records by each Record.Key in the order determined by sort algorithm.
func sortRecords(r *extract.Records, algo *SortAlgorithm) {
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
		for i := 0; i < r.Len(); i++ { // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
			j := rand.Intn(i + 1)
			r.Swap(i, j)
		}
	} else {
		sort.Sort(alphaByKey{r, algo.Decreasing, algo.FormatType})
	}
}
