// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
)

// ====================== How to run unit tests ===========================
//
// 1. Run all tests with default parameters
// $ go test -v
// 2. ... and debug enabled
// $ go test -v -tags=debug
// 3. ... and deadbeef (build tag) enabled, to "DEADBEEF" every freed buffer
// $ go test -v -tags=debug,deadbeef
// 4. Run a given named test with the specified build tags for 100s
// $ go test -v -tags=debug,deadbeef -run=Test_Sleep -duration=100s

// ============== Memory Manager Slab Allocator (MMSA) ===========================
//
// MMSA is, simultaneously, a Slab and SGL allocator, and a memory manager
// responsible to optimize memory usage between different (more vs less) utilized
// Slabs.
//
// Multiple MMSA instances may coexist in the system, each having its own
// constraints and managing its own Slabs and SGLs.
//
// There will be use cases, however, when actually running a MMSA instance
// won't be necessary: e.g., when an app utilizes a single (or a few distinct)
// Slab size(s) for the duration of its relatively short lifecycle,
// while at the same time preferring minimal interference with other running apps.
//
// In that sense, a typical initialization sequence includes 2 steps, e.g.:
// 1) construct:
// 	mm := &memsys.MMSA{TimeIval: ..., MinPctFree: ..., Name: ...}
// 2) initialize:
// 	err := mm.Init()
// 	if err != nil {
//		...
// 	}
//
// To free up all memory allocated by a given MMSA instance, use its Terminate() method.
//
// In addition, there are several environment variables that can be used
// (to circumvent the need to change the code, for instance):
// 	"AIS_MINMEM_FREE"
// 	"AIS_MINMEM_PCT_TOTAL"
// 	"AIS_MINMEM_PCT_FREE"
// These names must be self-explanatory.
//
// Once constructed and initialized, memory-manager-and-slab-allocator
// (MMSA) can be exercised via its public API that includes
// GetSlab() and Alloc*() methods.
//
// Once selected, each Slab instance can be used via its own public API that
// includes Alloc() and Free() methods. In addition, each allocated SGL internally
// utilizes one of the existing enumerated slabs to "grow" (that is, allocate more
// buffers from the slab) on demand. For details, look for "grow" in the iosgl.go.

const readme = cmn.GitHubHome + "/blob/main/memsys/README.md"

// =================================== tunables ==========================================
// The minimum memory (that must remain available) gets computed as follows:
// 1) environment AIS_MINMEM_FREE takes precedence over everything else;
// 2) if AIS_MINMEM_FREE is not defined, environment variables AIS_MINMEM_PCT_TOTAL and/or
//    AIS_MINMEM_PCT_FREE define percentages to compute the minimum based on total
//    or the currently available memory, respectively;
// 3) with no environment, the minimum is computed based on the following MMSA member variables:
//	* MinFree     uint64        // memory that must be available at all times
//	* MinPctTotal int           // same, via percentage of total
//	* MinPctFree  int           // ditto, as % of free at init time
//     (example:
//         mm := &memsys.MMSA{MinPctTotal: 4, MinFree: cos.GiB * 2}
//     )
//  4) finally, if none of the above is specified, the constant `minMemFree` below is used
//  Other important defaults are also commented below.
// =================================== MMSA config defaults ==========================================

const (
	PageSize            = cos.KiB * 4
	DefaultBufSize      = PageSize * 8
	DefaultBuf2Size     = PageSize * 16
	DefaultSmallBufSize = cos.KiB
)

// page slabs: pagesize increments up to MaxPageSlabSize
const (
	MaxPageSlabSize = 128 * cos.KiB
	PageSlabIncStep = PageSize
	NumPageSlabs    = MaxPageSlabSize / PageSlabIncStep // = 32
)

// small slabs: 128 byte increments up to MaxSmallSlabSize
const (
	MaxSmallSlabSize = PageSize
	SmallSlabIncStep = 128
	NumSmallSlabs    = MaxSmallSlabSize / SmallSlabIncStep // = 32
)

const NumStats = NumPageSlabs // NOTE: must be >= NumSmallSlabs

const (
	optDepth = 128  // ring "depth", i.e., num free bufs we trend to (see grow())
	minDepth = 4    // depth when idle or under OOM
	maxDepth = 4096 // exceeding warrants reallocation
)

const countThreshold = 16 // exceeding this scatter-gather count warrants selecting a larger-(buffer)-size Slab

const swappingMax = 4 // make sure that `swapping` condition, once noted, lingers for a while

type (
	MMSA struct {
		// public
		MinFree     uint64        // memory that must be available at all times
		TimeIval    time.Duration // interval of time to watch for low memory and make steps
		MinPctTotal int           // same, via percentage of total
		MinPctFree  int           // ditto, as % of free at init time
		Name        string
		// private
		info        string
		sibling     *MMSA
		rings       []*Slab
		hits        [NumStats]atomic.Uint64
		idleTs      [NumStats]atomic.Int64
		idleDur     [NumStats]time.Duration
		lowWM       uint64
		slabIncStep int64
		maxSlabSize int64
		defBufSize  int64
		mem         sys.MemStat
		numSlabs    int
		// atomic state
		toGC     atomic.Int64 // accumulates over time and triggers GC upon reaching spec-ed limit
		optDepth atomic.Int64 // ring "depth", i.e., num free bufs we trend to (see grow())
		swap     struct {
			size atomic.Uint64 // actual swap size
			crit atomic.Int32  // tracks increasing swap size up to `swappingMax`
		}
	}
	FreeSpec struct {
		MinSize int64 // minimum freed size that'd warrant calling GC (default = sizetoGC)
		ToOS    bool  // GC and then return the memory to the operating system
	}
)

//////////
// MMSA //
//////////

func (r *MMSA) String() string {
	var (
		mem sys.MemStat
		err error
	)
	err = mem.Get()
	debug.AssertNoErr(err)
	return r.Str(&mem)
}

func (r *MMSA) Str(mem *sys.MemStat) string {
	if r.info == "" {
		r.info = "(min-free " + cos.ToSizeIEC(int64(r.MinFree), 0) + ", low-wm " + cos.ToSizeIEC(int64(r.lowWM), 0)
	}

	var (
		sb strings.Builder
	)
	sb.Grow(80)
	sb.WriteString(r.Name)
	sb.WriteString("[(")
	mem.Str(&sb)
	sb.WriteString("), ")
	r._p2s(&sb, mem)
	sb.WriteString(", ")
	sb.WriteString(r.info)
	sb.WriteString("]")

	return sb.String()
}

// allocate SGL
//   - immediateSize: known size, OR minimum expected size, OR size to preallocate
//     immediateSize == 0 translates as DefaultBufSize - for page MMSA,
//     and DefaultSmallBufSize - for small-size MMSA
//   - sbufSize: slab buffer size (optional)
func (r *MMSA) NewSGL(immediateSize int64, sbufSize ...int64) *SGL {
	var (
		slab *Slab
		n    int64
		err  error
	)
	// 1. slab
	switch {
	case len(sbufSize) > 0:
		slab, err = r.GetSlab(sbufSize[0])
	case immediateSize <= r.maxSlabSize:
		// NOTE allocate imm. size in one shot when below max
		if immediateSize == 0 {
			immediateSize = r.defBufSize
		}
		i := cos.DivCeil(immediateSize, r.slabIncStep)
		slab = r.rings[i-1]
	default:
		slab = r._large2slab(immediateSize)
	}
	debug.AssertNoErr(err)

	// 2. sgl
	z := _allocSGL(r.isPage())
	z.slab = slab
	n = cos.DivCeil(immediateSize, slab.Size())
	if cap(z.sgl) < int(n) {
		z.sgl = make([][]byte, n)
	} else {
		z.sgl = z.sgl[:n]
	}
	slab.muget.Lock()
	for i := range int(n) {
		z.sgl[i] = slab._alloc()
	}
	slab.muget.Unlock()
	return z
}

// gets Slab for a given fixed buffer size that must be within expected range of sizes
// - the range supported by _this_ MMSA (compare w/ SelectMemAndSlab())
func (r *MMSA) GetSlab(bufSize int64) (s *Slab, err error) {
	a, b := bufSize/r.slabIncStep, bufSize%r.slabIncStep
	if b != 0 {
		err = fmt.Errorf("memsys: size %d must be a multiple of %d", bufSize, r.slabIncStep)
		return
	}
	if a < 1 || a > int64(r.numSlabs) {
		err = fmt.Errorf("memsys: size %d outside valid range", bufSize)
		return
	}
	s = r.rings[a-1]
	return
}

// uses SelectMemAndSlab to select both MMSA (page or small) and its Slab
func (r *MMSA) AllocSize(size int64) (buf []byte, slab *Slab) {
	_, slab = r.SelectMemAndSlab(size)
	buf = slab.Alloc()
	return
}

func (r *MMSA) Alloc() (buf []byte, slab *Slab) {
	_, slab = r.SelectMemAndSlab(r.defBufSize)
	buf = slab.Alloc()
	return
}

func (r *MMSA) Free(buf []byte) {
	size := int64(cap(buf))
	switch {
	case size > r.maxSlabSize && !r.isPage():
		r.sibling.Free(buf)
	case size < r.slabIncStep && r.isPage():
		r.sibling.Free(buf)
	default:
		slab := r._selectSlab(size)
		slab.Free(buf)
	}
}

// Given a known, expected or minimum size to allocate, selects MMSA (page or small, if initialized)
// and its Slab
func (r *MMSA) SelectMemAndSlab(size int64) (mmsa *MMSA, slab *Slab) {
	if size > r.maxSlabSize && !r.isPage() {
		return r.sibling, r.sibling._selectSlab(size)
	}
	if size < r.slabIncStep && r.isPage() {
		return r.sibling, r.sibling._selectSlab(size)
	}
	mmsa, slab = r, r._selectSlab(size)
	return
}

func (r *MMSA) _selectSlab(size int64) (slab *Slab) {
	switch {
	case size >= r.maxSlabSize:
		slab = r.rings[len(r.rings)-1]
	case size <= r.slabIncStep:
		slab = r.rings[0]
	default:
		i := (size + r.slabIncStep - 1) / r.slabIncStep
		slab = r.rings[i-1]
	}
	return
}

func (r *MMSA) AppendB(buf []byte, b byte) (nbuf []byte) {
	ll, c := len(buf), cap(buf)
	if ll+1 > c {
		nbuf = r._grow(buf, int64(c+1))
		nbuf = nbuf[:ll+1]
	} else {
		nbuf = buf[:ll+1]
	}
	nbuf[ll] = b
	return
}

func (r *MMSA) _grow(buf []byte, size int64) (nbuf []byte) {
	nbuf, _ = r.AllocSize(size) // not returning slab - no need
	copy(nbuf, buf)
	r.Free(buf)
	return
}

func (r *MMSA) AppendBytes(buf, bytes []byte) (nbuf []byte) {
	var (
		ll, l, c = len(buf), len(bytes), cap(buf)
		a        = ll + l - c
	)
	if a > 0 {
		nbuf = r._grow(buf, int64(c+a))
		nbuf = nbuf[:ll+l]
	} else {
		nbuf = buf[:ll+l]
	}
	copy(nbuf[ll:], bytes)
	return
}

func (r *MMSA) AppendString(buf []byte, s string) (nbuf []byte) {
	return r.AppendBytes(buf, cos.UnsafeB(s))
}

// private

// select slab for SGL given a large immediate size to allocate
func (r *MMSA) _large2slab(immediateSize int64) *Slab {
	size := cos.DivCeil(immediateSize, countThreshold)
	for _, slab := range r.rings {
		if slab.Size() >= size {
			return slab
		}
	}
	return r.rings[len(r.rings)-1]
}

func (r *MMSA) env() (err error) {
	var minfree int64
	if a := os.Getenv("AIS_MINMEM_FREE"); a != "" {
		if minfree, err = cos.ParseSize(a, cos.UnitsIEC); err != nil {
			return fmt.Errorf("memsys: cannot parse AIS_MINMEM_FREE %q", a)
		}
		r.MinFree = uint64(minfree)
	}
	if a := os.Getenv("AIS_MINMEM_PCT_TOTAL"); a != "" {
		if r.MinPctTotal, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("memsys: cannot parse AIS_MINMEM_PCT_TOTAL %q", a)
		}
		if r.MinPctTotal < 0 || r.MinPctTotal > 100 {
			return fmt.Errorf("memsys: invalid AIS_MINMEM_PCT_TOTAL %q", a)
		}
	}
	if a := os.Getenv("AIS_MINMEM_PCT_FREE"); a != "" {
		if r.MinPctFree, err = strconv.Atoi(a); err != nil {
			return fmt.Errorf("memsys: cannot parse AIS_MINMEM_PCT_FREE %q", a)
		}
		if r.MinPctFree < 0 || r.MinPctFree > 100 {
			return fmt.Errorf("memsys: invalid AIS_MINMEM_PCT_FREE %q", a)
		}
	}
	return
}
