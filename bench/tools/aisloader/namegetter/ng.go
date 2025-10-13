// Package namegetter is a utility to generate filenames for aisloader PUT/GET requests
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
		// PickBatch returns up to n unique names (within the returned batch).
		// The result is capped to min(n, len(names)).
		PickBatch(int, []string) []string
		Names() []string
		Len() int
	}

	// extend Basic with ability to add names
	Dynamic interface {
		Basic
		Add(objName string)
	}

	base struct {
		names []string
	}

	Random struct {
		base
		rnd  *rand.Rand
		seen bitSet // batch-level unqueness (here and elsewhere)
	}

	RandomUnique struct {
		base
		rnd     *rand.Rand
		seen    bitSet
		tracker bitmaskTracker // unqueness across epochs (here and elsewhere)
	}

	RandomUniqueIter struct {
		base
		rnd     *rand.Rand
		seen    bitSet
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

// with-replacement for single Pick(), but strictly unique within a batch
func (rng *Random) PickBatch(n int, in []string) []string {
	total := len(rng.names)
	n = min(total, n)

	rng.seen.reinit(total)
	for len(in) < n {
		idx := rng.rnd.IntN(total)
		if !rng.seen.testAndSet(idx) {
			continue
		}
		in = append(in, rng.names[idx])
	}
	return in
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

// strictly unique within the returned batch; still advances the epoch tracker
func (rng *RandomUnique) PickBatch(n int, in []string) []string {
	total := len(rng.names)
	n = min(total, n)

	rng.seen.reinit(total)
	if rng.tracker.needsReset(total) {
		rng.tracker.reset()
	}
	for len(in) < n {
		if rng.tracker.needsReset(total) {
			rng.tracker.reset()
		}
		for {
			idx := rng.rnd.IntN(total)
			if !rng.tracker.tryMark(idx) {
				// exhausted or collision; maybe epoch is done
				if rng.tracker.needsReset(total) {
					rng.tracker.reset()
				}
				continue
			}
			if !rng.seen.testAndSet(idx) {
				// already in this batch; keep going
				continue
			}
			in = append(in, rng.names[idx])
			break
		}
	}
	return in
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

// strictly unique within the returned batch; iterates in cyclic order from random start
func (rng *RandomUniqueIter) PickBatch(n int, in []string) []string {
	total := len(rng.names)
	n = min(total, n)

	rng.seen.reinit(total)
	if rng.tracker.needsReset(total) {
		rng.tracker.reset()
	}
	start := rng.rnd.IntN(total)
	for len(in) < n {
		// single pass over corpus
		for i := 0; i < total && len(in) < n; i++ {
			idx := start + i
			if idx >= total {
				idx -= total
			}
			if rng.tracker.tryMark(idx) && rng.seen.testAndSet(idx) {
				in = append(in, rng.names[idx])
			}
		}
		// epoch exhausted; reset and choose a new start if still need more
		if len(in) < n {
			rng.tracker.reset()
			start = rng.rnd.IntN(total)
		}
	}
	return in
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

// consecutive window from current permutation; unique in-batch
func (rng *Permutation) PickBatch(n int, in []string) []string {
	total := len(rng.names)
	n = min(total, n)
	remaining := n
	for remaining > 0 {
		left := total - rng.permidx
		if left == 0 {
			rng.permidx = 0
			rng.perm = rng.rnd.Perm(total)
			left = total
		}
		take := left
		if remaining < take {
			take = remaining
		}
		for i := range take {
			in = append(in, rng.names[rng.perm[rng.permidx+i]])
		}
		rng.permidx += take
		remaining -= take
	}
	return in
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

// consecutive window with pre-generated wrap
func (rng *PermutationImproved) PickBatch(n int, in []string) []string {
	total := len(rng.names)
	n = min(total, n)

	for remaining := n; remaining > 0; {
		left := total - rng.permidx
		if left == 0 {
			// swap in pre-generated next permutation
			rng.nextReady.Wait()
			rng.perm, rng.permNext = rng.permNext, rng.perm
			rng.permidx = 0

			// pre-generate the next one in background
			rng.nextReady.Add(1)
			go func(nLocal int) {
				r := cos.NowRand() // independent rng
				rng.permNext = r.Perm(nLocal)
				rng.nextReady.Done()
			}(total)
			left = total
		}
		take := left
		if remaining < take {
			take = remaining
		}
		for i := range take {
			in = append(in, rng.names[rng.perm[rng.permidx+i]])
		}
		rng.permidx += take
		remaining -= take
	}
	return in
}
