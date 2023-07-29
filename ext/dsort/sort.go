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
	algDefault   = ""             // default (alphanumeric, decreasing)
	Alphanumeric = "alphanumeric" // string comparison (decreasing or increasing)
	None         = "none"         // none (used for resharding)
	MD5          = "md5"          // compare md5(name)
	Shuffle      = "shuffle"      // random shuffle (use with the same seed to reproduce)
	Content      = "content"      // extract (int, string, float) from a given file, and compare
)

var algorithms = []string{algDefault, Alphanumeric, MD5, Shuffle, Content, None}

type Algorithm struct {
	// one of the `algorithms` above
	Kind string `json:"kind"`

	// used with two sorting alg-s: Alphanumeric and Content
	Decreasing bool `json:"decreasing"`

	// when sort is a random shuffle
	Seed string `json:"seed"`

	// exclusively with Content sorting
	// e.g. usage: ".cls" to provide sorting key for each record (sample) - see next
	Ext string `json:"extension"`

	// ditto: Content only
	// `extract.contentKeyTypes` enum values: {"int", "string", "float" }
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
func sortRecords(r *extract.Records, alg *Algorithm) (err error) {
	if alg.Kind == None {
		return nil
	}
	if alg.Kind == Shuffle {
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
	} else {
		keys := &alphaByKey{records: r, decreasing: alg.Decreasing, keyType: alg.ContentKeyType, err: nil}
		sort.Sort(keys)
		if keys.err != nil {
			return keys.err
		}
	}

	return nil
}
