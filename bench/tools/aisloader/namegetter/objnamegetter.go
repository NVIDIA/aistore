// Package namegetter is utility to generate filenames for aisloader PUT requests
/*
* Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package namegetter

import (
	"math/rand/v2"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	// base interface for read-only name selection
	Basic interface {
		Init(names []string, rnd *rand.Rand)
		// runtime
		Pick() string
		Names() []string
		Len() int
	}

	// extend Basic with ability to add names
	Dynamic interface {
		Basic
		Add(objName string)
	}

	// track indices used for unique selection
	bitmaskTracker struct {
		mask []uint64
		used int
	}

	base struct {
		names []string
	}

	Random struct {
		base
		rnd *rand.Rand
	}

	RandomUnique struct {
		base
		rnd     *rand.Rand
		tracker bitmaskTracker
	}

	RandomUniqueIter struct {
		base
		rnd     *rand.Rand
		tracker bitmaskTracker
	}

	Permutation struct {
		base
		rnd     *rand.Rand
		perm    []int
		permidx int
	}

	PermutationImproved struct {
		base
		rnd       *rand.Rand
		perm      []int
		permNext  []int
		permidx   int
		nextReady sync.WaitGroup
	}
)

////////////////////
// bitmaskTracker //
////////////////////

func (bt *bitmaskTracker) init(size int) {
	words := (size + 63) >> 6 // ceil(size/64)
	bt.mask = make([]uint64, words)
	bt.used = 0
}

func (bt *bitmaskTracker) grow() {
	bt.mask = append(bt.mask, 0)
}

// fast path
func (bt *bitmaskTracker) tryMark(idx int) bool {
	w := idx >> 6                // idx / 64
	b := uint64(1) << (idx & 63) // 1 << (idx % 64)
	if bt.mask[w]&b != 0 {
		return false
	}
	bt.mask[w] |= b
	bt.used++
	return true
}

func (bt *bitmaskTracker) reset() {
	for i := range bt.mask {
		bt.mask[i] = 0
	}
	bt.used = 0
}

func (bt *bitmaskTracker) needsReset(totalSize int) bool {
	return bt.used >= totalSize
}

//////////
// base //
//////////

func (bng *base) Names() []string {
	return bng.names
}

func (bng *base) Len() int {
	if bng == nil || bng.names == nil {
		return 0
	}
	return len(bng.names)
}

////////////
// Random //
////////////

// interface guard
var _ Dynamic = (*Random)(nil)

func (rng *Random) Init(names []string, rnd *rand.Rand) {
	rng.names = names
	rng.rnd = rnd
}

func (rng *Random) Add(objName string) {
	rng.names = append(rng.names, objName)
}

func (rng *Random) Pick() string {
	n := len(rng.names)
	debug.Assert(n > 0) // see objnameGetter.Len() here and elsewhere
	idx := rng.rnd.IntN(n)
	return rng.names[idx]
}

//////////////////
// RandomUnique //
//////////////////

// interface guard
var _ Dynamic = (*RandomUnique)(nil)

func (rng *RandomUnique) Init(names []string, rnd *rand.Rand) {
	rng.names = names
	rng.rnd = rnd
	rng.tracker.init(len(names))
}

func (rng *RandomUnique) Add(objName string) {
	if len(rng.names)%64 == 0 {
		rng.tracker.grow()
	}
	rng.names = append(rng.names, objName)
}

func (rng *RandomUnique) Pick() string {
	n := len(rng.names)
	debug.Assert(n > 0)
	if rng.tracker.needsReset(n) {
		rng.tracker.reset()
	}
	for {
		idx := rng.rnd.IntN(n)
		if rng.tracker.tryMark(idx) {
			return rng.names[idx]
		}
	}
}

//////////////////////
// RandomUniqueIter //
//////////////////////

// interface guard
var _ Dynamic = (*RandomUniqueIter)(nil)

func (rng *RandomUniqueIter) Init(names []string, rnd *rand.Rand) {
	rng.names = names
	rng.rnd = rnd
	rng.tracker.init(len(names))
}

func (rng *RandomUniqueIter) Add(objName string) {
	if len(rng.names)%64 == 0 {
		rng.tracker.grow()
	}
	rng.names = append(rng.names, objName)
}

func (rng *RandomUniqueIter) Pick() string {
	n := len(rng.names)
	debug.Assert(n > 0)
	if rng.tracker.needsReset(n) {
		rng.tracker.reset()
	}

	start := rng.rnd.IntN(n)
	for i := range n {
		idx := start + i
		if idx >= n {
			idx -= n
		}
		if rng.tracker.tryMark(idx) {
			return rng.names[idx]
		}
	}

	// everything was marked; reset and take one
	rng.tracker.reset()
	idx := rng.rnd.IntN(n)
	_ = rng.tracker.tryMark(idx)
	return rng.names[idx]
}

/////////////////
// Permutation //
/////////////////

// interface guard
var _ Basic = (*Permutation)(nil)

func (rng *Permutation) Init(names []string, rnd *rand.Rand) {
	rng.names = names
	rng.rnd = rnd
	rng.perm = rnd.Perm(len(names))
	rng.permidx = 0
}

func (*Permutation) Add(string) {
	cos.AssertMsg(false, "can't add object once Permutation is initialized")
}

func (rng *Permutation) Pick() string {
	n := len(rng.names)
	debug.Assert(n > 0)
	if rng.permidx == n {
		rng.permidx = 0
		rng.perm = rng.rnd.Perm(n)
	}
	objName := rng.names[rng.perm[rng.permidx]]
	rng.permidx++
	return objName
}

/////////////////////////
// PermutationImproved //
/////////////////////////

// interface guard
var _ Basic = (*PermutationImproved)(nil)

func (rng *PermutationImproved) Init(names []string, rnd *rand.Rand) {
	rng.nextReady.Wait() // handle double Init() edge case
	rng.names = names
	rng.rnd = rnd
	rng.perm = rnd.Perm(len(names))
	rng.permNext = rnd.Perm(len(names))
	rng.permidx = 0
}

func (*PermutationImproved) Add(string) {
	cos.AssertMsg(false, "can't add object once PermutationImproved is initialized")
}

func (rng *PermutationImproved) Pick() string {
	n := len(rng.names)
	debug.Assert(n > 0)
	if rng.permidx == n {
		rng.nextReady.Wait()
		rng.perm, rng.permNext = rng.permNext, rng.perm
		rng.permidx = 0

		// Pre-generate next permutation in background
		rng.nextReady.Add(1)
		go func() {
			// use independent rand
			r := cos.NowRand()
			rng.permNext = r.Perm(n)
			rng.nextReady.Done()
		}()
	}

	objName := rng.names[rng.perm[rng.permidx]]
	rng.permidx++
	return objName
}
