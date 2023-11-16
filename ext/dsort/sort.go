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
	"github.com/NVIDIA/aistore/ext/dsort/shard"
)

type (
	alphaByKey struct {
		err        error
		records    *shard.Records
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

// sorts records by each Record.Key in the order determined by the `alg` algorithm.
func sortRecords(r *shard.Records, alg *Algorithm) (err error) {
	switch alg.Kind {
	case None:
		return nil
	case Shuffle:
		var (
			rnd  *rand.Rand
			seed = time.Now().Unix()
		)
		if alg.Seed != "" {
			seed, err = strconv.ParseInt(alg.Seed, 10, 64)
			debug.AssertNoErr(err)
		}
		rnd = rand.New(rand.NewSource(seed))
		for i := 0; i < r.Len(); i++ { // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
			j := rnd.Intn(i + 1)
			r.Swap(i, j)
		}
	default:
		keys := &alphaByKey{records: r, decreasing: alg.Decreasing, keyType: alg.ContentKeyType}
		sort.Sort(keys)
		err = keys.err
	}
	return
}
