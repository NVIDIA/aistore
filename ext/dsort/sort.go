// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
)

const (
	sortKindEmpty        = ""             // default one - alphanumeric (sort decreasing)
	SortKindAlphanumeric = "alphanumeric" // sort the records (decreasing or increasing)
	SortKindNone         = "none"         // none, used for resharding
	SortKindMD5          = "md5"
	SortKindShuffle      = "shuffle" // shuffle randomly, can be used with seed to get reproducible results
	SortKindContent      = "content" // sort by content of given file
)

const (
	fmtInvalidAlgorithmKind = "invalid algorithm kind, expecting one of: %+v" // <--- supportedAlgorithms
)

var supportedAlgorithms = []string{sortKindEmpty, SortKindAlphanumeric, SortKindMD5, SortKindShuffle, SortKindContent, SortKindNone}

type (
	alphaByKey struct {
		*extract.Records
		decreasing bool
		formatType string
		err        error
	}
)

// interface guard
var _ sort.Interface = (*alphaByKey)(nil)

func (s *alphaByKey) Less(i, j int) bool {
	var (
		less bool
		err  error
	)

	if s.decreasing {
		less, err = s.Records.Less(j, i, s.formatType)
	} else {
		less, err = s.Records.Less(i, j, s.formatType)
	}

	if err != nil {
		s.err = err
	}
	return less
}

// sortRecords sorts records by each Record.Key in the order determined by sort algorithm.
func sortRecords(r *extract.Records, algo *SortAlgorithm) (err error) {
	var rnd *rand.Rand
	if algo.Kind == SortKindNone {
		return nil
	} else if algo.Kind == SortKindShuffle {
		seed := time.Now().Unix()
		if algo.Seed != "" {
			seed, err = strconv.ParseInt(algo.Seed, 10, 64)
			// We assert error since we know that the seed should be validated
			// during request spec validation.
			debug.AssertNoErr(err)
		}

		rnd = rand.New(rand.NewSource(seed))
		for i := 0; i < r.Len(); i++ { // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
			j := rnd.Intn(i + 1)
			r.Swap(i, j)
		}
	} else {
		keys := &alphaByKey{r, algo.Decreasing, algo.FormatType, nil}
		sort.Sort(keys)

		if keys.err != nil {
			return keys.err
		}
	}

	return nil
}
