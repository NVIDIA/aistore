// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"fmt"
	"math/bits"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"

	onexxh "github.com/OneOfOne/xxhash"
)

// A variant of consistent hash based on rendezvous algorithm by Thaler and Ravishankar,
// aka highest random weight (HRW)
//
// Invariants:
// - the result returned by the routines below depends on Smap state, not just membership:
//   InMaintOrDecomm nodes are skipped
//   (and at least in one case non-electable proxies are skipped as well)
// - object selectors break hash ties by target ID; proxy and task selectors retain
//   their original tie handling (a collision of 64-bit hashes is extremely unlikely).
// - HrwName2T and friends are uniform; HrwObj* apply bucket placement and target weights.
//
// See also: fs/hrw.go

func (smap *Smap) HrwName2T(uname []byte) (*Snode, error) {
	digest := onexxh.Checksum64S(uname, cos.MLCG32)
	return smap.HrwHash2T(digest)
}

// TODO: control plane multihoming: return LRU data plane interface

func (smap *Smap) HrwMultiHome(uname []byte) (si *Snode, netName string, err error) {
	digest := onexxh.Checksum64S(uname, cos.MLCG32)
	si, err = smap.HrwHash2T(digest)
	if err != nil {
		return nil, cmn.NetPublic, err
	}

	debug.Func(func() { debug.Assert(si.nmr != nil, si.StringEx(), " in ", smap.StringEx()) }) // see related: smapOwner.put
	return si, si.nmr.name(), nil
}

func (smap *Smap) HrwHash2T(digest uint64) (si *Snode, err error) {
	var maxH uint64
	for _, tsi := range smap.Tmap {
		if tsi.InMaintOrDecomm() { // always skipping targets 'in maintenance mode'
			continue
		}
		cs := xoshiro256.Hash(tsi.digest() ^ digest)
		if hrwHashGreater(cs, tsi, maxH, si) {
			maxH = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return si, err
}

// NOTE: including targets 'in maintenance mode', if any
func (smap *Smap) HrwHash2Tall(digest uint64) (si *Snode, err error) {
	var maxH uint64
	for _, tsi := range smap.Tmap {
		cs := xoshiro256.Hash(tsi.digest() ^ digest)
		if hrwHashGreater(cs, tsi, maxH, si) {
			maxH = cs
			si = tsi
		}
	}
	if si == nil {
		err = cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return si, err
}

func (smap *Smap) HrwProxy(idToSkip string) (pi *Snode, err error) {
	var maxH uint64
	for pid, psi := range smap.Pmap {
		if pid == idToSkip {
			continue
		}
		if psi.Flags.IsSet(SnodeNonElectable) {
			continue
		}
		if psi.InMaintOrDecomm() {
			continue
		}
		if d := psi.digest(); d >= maxH {
			maxH = d
			pi = psi
		}
	}
	if pi == nil {
		err = cmn.NewErrNoNodes(apc.Proxy, len(smap.Pmap))
	}
	return pi, err
}

func (smap *Smap) HrwIC(uuid string) (pi *Snode, err error) {
	var (
		maxH   uint64
		digest = onexxh.Checksum64S(cos.UnsafeB(uuid), cos.MLCG32)
	)
	for _, psi := range smap.Pmap {
		if psi.InMaintOrDecomm() || !psi.IsIC() {
			continue
		}
		cs := xoshiro256.Hash(psi.digest() ^ digest)
		if cs >= maxH {
			maxH = cs
			pi = psi
		}
	}
	if pi == nil {
		err = fmt.Errorf("IC is empty %s: %s", smap, smap.StrIC(nil))
	}
	return pi, err
}

// HRW over a given node map; skips unavailable nodes; returns nil when none qualifies.
func hrwOver(nodes NodeMap, uuid string) (si *Snode) {
	var (
		maxH   uint64
		digest = onexxh.Checksum64S(cos.UnsafeB(uuid), cos.MLCG32)
	)
	for _, nsi := range nodes {
		if nsi.InMaintOrDecomm() {
			continue
		}
		cs := xoshiro256.Hash(nsi.digest() ^ digest)
		if cs >= maxH {
			maxH = cs
			si = nsi
		}
	}
	return si
}

// return a (designated) target for a given task
// (e.g. usage: list objects in a cloud bucket - we want one DT to do it)
func (smap *Smap) HrwTargetTask(uuid string) (si *Snode, err error) {
	if si = hrwOver(smap.Tmap, uuid); si == nil {
		err = cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return si, err
}

// return a proxy for a given task - e.g., the proxy to own a given list-objects
// pagination (compare with HrwIC above: the latter selects from the IC members only)
func (smap *Smap) HrwProxyTask(uuid string) (si *Snode, err error) {
	if si = hrwOver(smap.Pmap, uuid); si == nil {
		err = cmn.NewErrNoNodes(apc.Proxy, len(smap.Pmap))
	}
	return si, err
}

/////////////
// hrwList //
/////////////

type hrwList struct {
	hs  []uint64
	sis Nodes
	n   int
}

// Sorts all targets in a cluster by their respective HRW (weights) in a descending order;
// returns resulting subset (aka slice) that has the requested length = count.
// Returns error if the cluster does not have enough targets.
// If count == length of Smap.Tmap, the function returns as many targets as possible.

func (smap *Smap) HrwTargetList(uname *string, count int) (sis Nodes, err error) {
	if cnt := smap.CountTargets(); cnt < count {
		return nil, cmn.NewErrNotEnoughTargets(fmt.Sprintf("required %d, available %d, %s", count, cnt, smap))
	}
	b := cos.UnsafeBptr(uname)
	digest := onexxh.Checksum64S(*b, cos.MLCG32)
	return smap.hrwTargetList(digest, count)
}

func (smap *Smap) hrwTargetList(digest uint64, count int) (sis Nodes, err error) {
	const fmterr = "required %d, available %d, %s"
	cnt := smap.CountTargets()
	if cnt < count {
		s := fmt.Sprintf(fmterr, count, cnt, smap)
		return nil, cmn.NewErrNotEnoughTargets(s)
	}
	hlist := newHrwList(count)

	for _, tsi := range smap.Tmap {
		cs := xoshiro256.Hash(tsi.digest() ^ digest)
		if tsi.InMaintOrDecomm() {
			continue
		}
		hlist.add(cs, tsi)
	}
	sis = hlist.get()
	if count != cnt && len(sis) < count {
		s := fmt.Sprintf(fmterr, count, len(sis), smap)
		return nil, cmn.NewErrNotEnoughTargets(s)
	}
	return sis, nil
}

func newHrwList(count int) *hrwList {
	return &hrwList{hs: make([]uint64, 0, count), sis: make(Nodes, 0, count), n: count}
}

func (hl *hrwList) get() Nodes { return hl.sis }

// Adds Snode with `weight`. The result is sorted on the fly with insertion sort
// and it makes sure that the length of resulting list never exceeds `count`
func (hl *hrwList) add(weight uint64, sinfo *Snode) {
	l := len(hl.sis)
	if l == hl.n && !hrwHashGreater(weight, sinfo, hl.hs[l-1], hl.sis[l-1]) {
		return
	}
	if l == hl.n {
		hl.hs[l-1] = weight
		hl.sis[l-1] = sinfo
	} else {
		hl.hs = append(hl.hs, weight)
		hl.sis = append(hl.sis, sinfo)
		l++
	}
	idx := l - 1
	for idx > 0 && hrwHashGreater(hl.hs[idx], hl.sis[idx], hl.hs[idx-1], hl.sis[idx-1]) {
		hl.hs[idx], hl.hs[idx-1] = hl.hs[idx-1], hl.hs[idx]
		hl.sis[idx], hl.sis[idx-1] = hl.sis[idx-1], hl.sis[idx]
		idx--
	}
}

// order equal scores by node ID, independently of map iteration order
func hrwHashGreater(hash uint64, si *Snode, otherHash uint64, other *Snode) bool {
	return hash >= otherHash && (hash > otherHash || other == nil || si.ID() < other.ID())
}

//////////////////
// weighted HRW //
//////////////////

// Traditional HRW generates a uniform random score for each target using a hash function,
// and picks the maximum score. To introduce weights, the algorithm below transforms the
// uniform random variables U in (0,1] into exponentially distributed -ln(U)/weight, and
// looks for the minimum:
// - P(-ln(U)/w > x) = exp(-w*x); the minimum of independent exponentials falls on target i
//   with probability w(i)/sum(w), hence proportional shares
// - equal weights: -ln is decreasing, so the minimum is the traditional maximum
//   (compare hashes directly - no logarithms)
// - proportional weights scale all scores equally: same placement
// - changing one target's weight moves objects only to or from that target
// - ranking by score is weighted sampling without replacement: the first is the owner,
//   and shorter lists are prefixes of longer ones (see HrwObjTargetList)
//
// Integer-only arithmetic - the same results on every architecture:
// - hrwLog: -ln(U) in fixed point (Q56)
// - compare L1/w1 vs L2/w2 as L1*w2 vs L2*w1 (128-bit products): exact for any int64 weights
// - hrwLogLB: lower bound to skip targets that cannot win

type hrwScore struct {
	si     *Snode
	hash   uint64
	log    uint64 // lazy Q56 -ln(U); zero means not computed
	weight int64
}

// probe one target: all are weighted, or none (see weight.go)
func (smap *Smap) weighted() bool {
	for _, tsi := range smap.Tmap {
		return tsi.Placement != nil
	}
	return false
}

func (smap *Smap) hrwWeightedHash2T(digest uint64, all bool) (*Snode, error) {
	var best hrwScore
	for _, tsi := range smap.Tmap {
		if !all && tsi.InMaintOrDecomm() {
			continue
		}
		w := tsi.PlacementWeight()
		if w <= 0 {
			return nil, fmt.Errorf("invalid placement weights in %s: require all positive or all zero (%s=%d)", smap, tsi, w)
		}
		score := hrwScore{si: tsi, hash: xoshiro256.Hash(tsi.digest() ^ digest), weight: w}
		if best.si == nil || score.better(&best) {
			best = score
		}
	}
	if best.si == nil {
		return nil, cmn.NewErrNoNodes(apc.Target, len(smap.Tmap))
	}
	return best.si, nil
}

func (smap *Smap) hrwWeightedTargetList(digest uint64, count int) (Nodes, error) {
	const fmterr = "required %d, available %d, %s"
	cnt := smap.CountTargets()
	if cnt < count {
		return nil, cmn.NewErrNotEnoughTargets(fmt.Sprintf(fmterr, count, cnt, smap))
	}
	ranked := make([]hrwScore, 0, count)
	for _, tsi := range smap.Tmap {
		if tsi.InMaintOrDecomm() {
			continue
		}
		w := tsi.PlacementWeight()
		if w <= 0 {
			return nil, fmt.Errorf("invalid placement weights in %s: require all positive or all zero (%s=%d)", smap, tsi, w)
		}
		score := hrwScore{si: tsi, hash: xoshiro256.Hash(tsi.digest() ^ digest), weight: w}
		n := len(ranked)
		if n == count {
			if !score.better(&ranked[n-1]) {
				continue
			}
			ranked[n-1] = score
		} else {
			ranked = append(ranked, score)
			n++
		}
		for i := n - 1; i > 0 && ranked[i].better(&ranked[i-1]); i-- {
			ranked[i], ranked[i-1] = ranked[i-1], ranked[i]
		}
	}
	if count != cnt && len(ranked) < count {
		return nil, cmn.NewErrNotEnoughTargets(fmt.Sprintf(fmterr, count, len(ranked), smap))
	}
	sis := make(Nodes, len(ranked))
	for i := range ranked {
		sis[i] = ranked[i].si
	}
	return sis, nil
}

// compare scores, caching logarithms only when weights differ
func (s *hrwScore) better(other *hrwScore) bool {
	if s.weight == other.weight {
		return hrwHashGreater(s.hash, s.si, other.hash, other.si)
	}
	if other.log == 0 {
		other.log = hrwLog(other.hash)
	}
	if s.log == 0 {
		// skip the logarithm when its lower bound already loses
		hi, lo := bits.Mul64(hrwLogLB(s.hash), uint64(other.weight))
		ohi, olo := bits.Mul64(other.log, uint64(s.weight))
		if hi > ohi || (hi == ohi && lo > olo) {
			return false
		}
		s.log = hrwLog(s.hash)
	}
	hi, lo := bits.Mul64(s.log, uint64(other.weight))
	ohi, olo := bits.Mul64(other.log, uint64(s.weight))
	if hi != ohi {
		return hi < ohi
	}
	if lo != olo {
		return lo < olo
	}
	return hrwHashGreater(s.hash, s.si, other.hash, other.si)
}

// approximate -ln((hash+1)/2^64) in Q56, clamping the upper endpoint to one unit
// - normalize hash+1 to m in [1,2), then ln(m) = 2*(y + y^3/3 + ...), y=(m-1)/(m+1)
// - y < 1/3: 18 terms bound the series remainder below 2^-62
// - intermediate Q63 arithmetic retains seven guard bits; the result fits in 62 bits
// - integer operations specify the same rounding on every architecture
func hrwLog(hash uint64) uint64 {
	const ln2Q63 = uint64(6393154322601327829)
	x := hash + 1
	if x == 0 {
		return 1
	}
	e := bits.LeadingZeros64(x)
	m := x << e >> 1 // Q62 m in [1,2)
	num, den := m-(1<<62), m+(1<<62)
	y, _ := bits.Div64(num>>1, num<<63, den) // Q63 (m-1)/(m+1)
	y2 := hrwMulQ63(y, y)
	coeff := [...]uint64{
		1 << 63, (1 << 63) / 3, (1 << 63) / 5, (1 << 63) / 7,
		(1 << 63) / 9, (1 << 63) / 11, (1 << 63) / 13, (1 << 63) / 15,
		(1 << 63) / 17, (1 << 63) / 19, (1 << 63) / 21, (1 << 63) / 23,
		(1 << 63) / 25, (1 << 63) / 27, (1 << 63) / 29, (1 << 63) / 31,
		(1 << 63) / 33, (1 << 63) / 35,
	}
	p := coeff[len(coeff)-1]
	for i := len(coeff) - 2; i >= 0; i-- {
		p = coeff[i] + hrwMulQ63(y2, p)
	}
	lnm := 2 * hrwMulQ63(y, p)
	hi, lo := bits.Mul64(uint64(e+1), ln2Q63)
	lo, borrow := bits.Sub64(lo, lnm, 0)
	hi -= borrow
	return max(hi<<57|lo>>7, 1) // Q63 -> Q56
}

// return a lower bound of hrwLog: e*ln2 + table(next 8 bits), less a margin
// - hrwLog is within one unit of -ln, and -ln decreases in hash
// - table entries are hrwLog at the upper end of each 1/256 interval
const (
	hrwLn2Q56    = uint64(49946518145322873) // hrwLog(1<<63 - 1)
	hrwLogMargin = 1 << 10
)

var hrwLogFrac = func() (tab [256]uint64) {
	for t := range 255 {
		tab[t] = hrwLog(1<<63 + uint64(t+1)<<55 - 1)
	}
	return tab
}()

func hrwLogLB(hash uint64) uint64 {
	x := hash + 1
	if x == 0 {
		return 0
	}
	e := bits.LeadingZeros64(x)
	lb := uint64(e)*hrwLn2Q56 + hrwLogFrac[x<<e>>55&0xff]
	return lb - min(lb, hrwLogMargin)
}

func hrwMulQ63(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi<<1 | lo>>63
}

////////////////////////////
// bucket-aware placement //
////////////////////////////

// Object placement: bucket placement override and target weights.
// - nil bucket (or bucket properties): cluster default, i.e. weighted if the Smap is
// - initialize the bucket before calling to honor its override
// - name-based selectors require the bucket (it is part of the uname)

func (smap *Smap) HrwObjName2T(bck *Bck, objName string) (*Snode, error) {
	digest := onexxh.Checksum64S(bck.MakeUname(objName), cos.MLCG32)
	return smap.HrwObjHash2T(bck, digest)
}

// use the object's placement digest (LOM.Digest or CT.Digest)
func (smap *Smap) HrwObjHash2T(bck *Bck, digest uint64) (*Snode, error) {
	if bck.uniformPlacement() || !smap.weighted() {
		return smap.HrwHash2T(digest)
	}
	return smap.hrwWeightedHash2T(digest, false /*include inactive*/)
}

// include targets in maintenance or decommission (a recovery hint, as in HrwHash2Tall)
func (smap *Smap) HrwObjHash2Tall(bck *Bck, digest uint64) (*Snode, error) {
	if bck.uniformPlacement() || !smap.weighted() {
		return smap.HrwHash2Tall(digest)
	}
	return smap.hrwWeightedHash2T(digest, true)
}

func (smap *Smap) HrwObjMultiHome(bck *Bck, objName string) (*Snode, string, error) {
	si, err := smap.HrwObjName2T(bck, objName)
	if err != nil {
		return nil, cmn.NetPublic, err
	}
	debug.Func(func() { debug.Assert(si.nmr != nil, si.StringEx(), " in ", smap.StringEx()) })
	return si, si.nmr.name(), nil
}

// rank targets using the object's digest (the same digest as HrwObjHash2T)
// - the first entry is the object's owner; all entries are distinct and active
// - require a positive count
// - retain HrwTargetList's count and availability semantics
func (smap *Smap) HrwObjTargetList(bck *Bck, digest uint64, count int) (Nodes, error) {
	if count <= 0 {
		return nil, fmt.Errorf("invalid HRW target count %d (must be positive)", count)
	}
	if bck.uniformPlacement() || !smap.weighted() {
		return smap.hrwTargetList(digest, count)
	}
	return smap.hrwWeightedTargetList(digest, count)
}

// (nil-safe)
func (b *Bck) uniformPlacement() bool {
	return b != nil && b.Props != nil && b.Props.Placement != nil && b.Props.Placement.Weighting == cmn.WeightingUniform
}
