// Package namegetter is a utility to provide random object and archived file names to aisloader
/*
* Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package namegetter

import (
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	// base interface for read-only name selection
	Basic interface {
		Init(names []string, rnd *rand.Rand)

		// when mixed read/write
		// `Random` and `RandomUnique` implement Add()
		Add(objName string)

		// pick a random name
		Pick() string

		// iterate over a batch of up to `limit` unique names
		// - calls yield(name) for each selected name
		// - if yield returns false, iteration stops early
		// - if fewer names are available than limit, fewer names are yielded
		IterBatch(limit int, yield func(name string) bool)

		// all known names and the number of thereof
		// (note: both methods not thread-safe)
		Names() []string
		Len() int
	}

	base struct {
		names []string
	}

	Random struct {
		base
		rnd  *rand.Rand
		seen bitSet // batch-level uniqueness (here and elsewhere)
	}

	RandomUnique struct {
		base
		rnd     *rand.Rand
		seen    bitSet
		tracker bitmaskTracker // uniqueness across epochs (here and elsewhere)
	}

	// PermShuffle generates repeated random permutations of an open interval [0..n)
	// NOTE: IterBatch can cross a permutation boundary - if that happens
	// the yielded batch (of names) may contain duplicates (extended comment below)
	PermShuffle struct {
		base
		rnd       *rand.Rand
		perm      []uint32
		permNext  []uint32
		permidx   uint32
		nextReady sync.WaitGroup
	}

	// epoch-based affine sequence over nextPrimeGE(n);
	// each epoch emits exactly n unique indices; IterBatch never crosses epochs,
	// so duplicates within a single batch are impossible
	PermAffinePrime struct {
		base
		rnd  *rand.Rand
		n, p int // n=len(names), p=nextPrimeGE(n) >= 2
		a, b int // multiplier (1..p-1) and offset (0..p-1)
		out  int // how many outputs emitted in current epoch [0..n]
		curY int // current position in affine sequence
	}
)

//////////
// base //
//////////

func (rng *base) Names() []string { return rng.names }

func (rng *base) Len() int {
	if rng == nil {
		return 0
	}
	return len(rng.names)
}

func (rng *base) Add(string) {
	cos.Assertf(false, "this name-getter type %T cannot add names at runtime", rng)
}

////////////
// Random //
////////////

// interface guard
var _ Basic = (*Random)(nil)

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
func (rng *Random) IterBatch(limit int, yield func(name string) bool) {
	total := len(rng.names)
	n := min(total, limit)
	debug.Assert(n > 0)

	rng.seen.reinit(total)
	emitted := 0

	for emitted < n {
		idx := rng.rnd.IntN(total)
		if !rng.seen.testAndSet(idx) {
			continue
		}
		if !yield(rng.names[idx]) {
			return
		}
		emitted++
	}
}

//////////////////
// RandomUnique //
//////////////////

// interface guard
var _ Basic = (*RandomUnique)(nil)

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

// strictly unique within the yielded batch; still advances the epoch tracker
func (rng *RandomUnique) IterBatch(limit int, yield func(name string) bool) {
	total := len(rng.names)
	n := min(total, limit)
	debug.Assert(n > 0)

	rng.seen.reinit(total)
	if rng.tracker.needsReset(total) {
		rng.tracker.reset()
	}

	emitted := 0
	for emitted < n {
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
				continue
			}
			if !yield(rng.names[idx]) {
				return
			}
			emitted++
			break
		}
	}
}

/////////////////
// PermShuffle //
/////////////////

// interface guard
var _ Basic = (*PermShuffle)(nil)

func (rng *PermShuffle) Init(names []string, rnd *rand.Rand) {
	rng.nextReady.Wait() // handle double Init() edge case
	rng.names = names
	rng.rnd = rnd
	n := uint32(len(names))
	rng.perm = _shuffle(rnd, n)
	rng.permNext = _shuffle(rnd, n)
	rng.permidx = 0
}

func (rng *PermShuffle) Pick() string {
	n := uint32(len(rng.names))
	debug.Assert(n > 0)
	if rng.permidx == n {
		rng.nextReady.Wait()
		rng.perm, rng.permNext = rng.permNext, rng.perm
		rng.permidx = 0

		// Pre-generate next permutation in background
		rng.nextReady.Add(1)
		go func(num uint32) {
			r := cos.NowRand()
			rng.permNext = _shuffle(r, num)
			rng.nextReady.Done()
		}(n)
	}

	objName := rng.names[rng.perm[rng.permidx]]
	rng.permidx++
	return objName
}

// NOTE: =====================================================================
// * A single IterBatch can legitimately **straddle two permutations**.
//   When that happens, the same name can appear twice within one batch
//   (tail of previous permutation + head of the next).
//   In other words, uniqueness is guaranteed
//   only per permutation (epoch), not per individual IterBatch call.
// * Callers that require strictly unique names within each batch must either
//   (a) set batch size <= remaining items in the current permutation, or
//   (b) detect and skip duplicates at the boundary.
// ============================================================================

// consecutive window with pre-generated wrap
func (rng *PermShuffle) IterBatch(limit int, yield func(name string) bool) {
	total := uint32(len(rng.names))
	n := min(uint32(limit), total)
	emitted := uint32(0)

	for emitted < n {
		left := total - rng.permidx
		if left == 0 {
			// swap in pre-generated next permutation
			rng.nextReady.Wait()
			rng.perm, rng.permNext = rng.permNext, rng.perm
			rng.permidx = 0

			// pre-generate the next one in background
			rng.nextReady.Add(1)
			go func(num uint32) {
				r := cos.NowRand()
				rng.permNext = _shuffle(r, num)
				rng.nextReady.Done()
			}(total)
			left = total
		}

		// number of items to emit from current permutation window
		win := min(n-emitted, left)

		// yield window of names
		for j := range win {
			idx := rng.perm[rng.permidx+j]
			if !yield(rng.names[idx]) {
				// early termination
				rng.permidx += j
				return
			}
		}

		rng.permidx += win
		emitted += win
	}
}

// generate uint32 permutation
func _shuffle(rnd *rand.Rand, n uint32) []uint32 {
	if n < 2 {
		panic(fmt.Errorf("shuffle: need at least 2 items (got %d)", n))
	}
	perm := make([]uint32, n)
	for i := range n {
		perm[i] = i
	}
	// Fisher-Yates shuffle
	for i := int(n) - 1; i > 0; i-- {
		j := uint32(rnd.Int64N(int64(i + 1)))
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}

/////////////////////
// PermAffinePrime //
/////////////////////

const AffineMinN = 10 // avoid tiny datasets and related corners

var _ Basic = (*PermAffinePrime)(nil)

// Init: ensure n>0 and p>=2 once
func (pp *PermAffinePrime) Init(names []string, rnd *rand.Rand) {
	pp.names = names
	pp.rnd = rnd
	pp.n = len(names)
	pp.p = nextPrimeGE(pp.n)
	debug.Assert(pp.n > AffineMinN, "use a different name-getter for tiny datasets")
	pp.newEpoch()
}

// New epoch: pick a,b and seed current y; no max(1,p-1) needed now
func (pp *PermAffinePrime) newEpoch() {
	pp.out = 0
	pp.a = 1 + pp.rnd.IntN(pp.p-1)
	pp.b = pp.rnd.IntN(pp.p)
	pp.curY = pp.b
}

// Pick: increment y with add+wrap; no mul/mod in the hot path
func (pp *PermAffinePrime) Pick() string {
	n, p := pp.n, pp.p
	if pp.out == n {
		pp.newEpoch()
	}
	for {
		y := pp.curY
		// advance for next time
		ny := y + pp.a
		if ny >= p {
			ny -= p
		}
		pp.curY = ny

		if y < n {
			pp.out++
			return pp.names[y]
		}
	}
}

// epoch-based affine sequence over nextPrimeGE(n);
// each epoch emits exactly n unique indices
// NOTE:
// - may straddle epoch boundaries; in that case,
// - a single batch can contain duplicates (tail of previous epoch + head of the next);
// - unlikely though if the dataset size >> batch size
func (pp *PermAffinePrime) IterBatch(limit int, yield func(name string) bool) {
	debug.Assert(limit > 0 && limit <= pp.n)

	n := pp.n
	p := pp.p
	a := pp.a
	y := pp.curY
	emitted := 0
	for emitted < limit {
		// exhausted current epoch: start a new one
		if pp.out == n {
			pp.newEpoch()
			p = pp.p
			a = pp.a
			y = pp.curY
		}

		// for this epoch, emit only indices that map into [0..n)
		if y < n {
			if !yield(pp.names[y]) {
				// caller stopped early; persist state and return
				pp.curY = y
				return
			}
			emitted++
			pp.out++
		}

		// advance y = (y + a) mod p without %/mul
		y += a
		if y >= p {
			y -= p
		}
	}

	pp.curY = y
}

// --- primes ---

func nextPrimeGE(n int) int {
	debug.Assert(n > AffineMinN)
	if n%2 == 0 {
		n++
	}
	for n%3 == 0 || n%5 == 0 {
		n += 2
	}

	var (
		// store rid+1; 0 means not found
		residueIndex = [30]int{1: 1, 7: 2, 11: 3, 13: 4, 17: 5, 19: 6, 23: 7, 29: 8}
		res          = n % 30
		rid          = residueIndex[res]
	)
	if rid == 0 {
		// advance to next allowed residue without modulo
		for {
			n += 2
			res += 2
			if res >= 30 {
				res -= 30
			}
			if rid = residueIndex[res]; rid != 0 {
				break
			}
		}
	}

	var (
		idx   = rid - 1
		steps = [...]int{6, 4, 2, 4, 2, 4, 6, 2}
	)
	for {
		if isPrime30(n) {
			return n
		}
		n += steps[idx]
		idx++
		if idx == len(steps) {
			idx = 0
		}
	}
}

// a simple wheel (mod 30) sieve to find next prime >= n
// https://en.wikipedia.org/wiki/Wheel_factorization
// n > AffineMinN and odd
func isPrime30(n int) bool {
	if n%3 == 0 || n%5 == 0 {
		return false
	}
	for i := 7; i <= n/i; i += 30 {
		// unrolled checks for the 8 residues per 30
		if n%i == 0 ||
			n%(i+4) == 0 ||
			n%(i+6) == 0 ||
			n%(i+10) == 0 ||
			n%(i+12) == 0 ||
			n%(i+16) == 0 ||
			n%(i+22) == 0 ||
			n%(i+24) == 0 {
			return false
		}
	}
	return true
}
