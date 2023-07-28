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

type SortAlgorithm struct {
	// one of the `supportedAlgorithms` (see above)
	Kind string `json:"kind"`

	// currently, used with two sorting alg-s: SortKindAlphanumeric and SortKindContent
	Decreasing bool `json:"decreasing"`

	// when sort is a random shuffle
	Seed string `json:"seed"`

	// exclusively with sorting alg. Kind = "content" (aka SortKindContent)
	// used to select files that provide sorting keys - see next
	Extension string `json:"extension"`

	// ditto: SortKindContent only
	// one of extract.contentKeyTypes, namely: {"int", "string", ... }
	ContentKeyType string `json:"content_key_type"`
}

type (
	alphaByKey struct {
		err        error
		records    *extract.Records
		keyType    string
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
		less, err = s.records.Less(j, i, s.keyType)
	} else {
		less, err = s.records.Less(i, j, s.keyType)
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
		keys := &alphaByKey{records: r, decreasing: algo.Decreasing, keyType: algo.ContentKeyType, err: nil}
		sort.Sort(keys)

		if keys.err != nil {
			return keys.err
		}
	}

	return nil
}
