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
		err        error
		records    *extract.Records
		formatType string
		decreasing bool
	}
)

// interface guard
var _ sort.Interface = (*alphaByKey)(nil)

func (s *alphaByKey) Len() int      { return s.records.Len() }
func (s *alphaByKey) Swap(i, j int) { s.records.Swap(i, j) }

func (s *alphaByKey) Less(i, j int) bool {
	var (
		err  error
		less bool
	)
	if s.decreasing {
		less, err = s.records.Less(j, i, s.formatType)
	} else {
		less, err = s.records.Less(i, j, s.formatType)
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
		keys := &alphaByKey{records: r, decreasing: algo.Decreasing, formatType: algo.FormatType, err: nil}
		sort.Sort(keys)

		if keys.err != nil {
			return keys.err
		}
	}

	return nil
}
