// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
)

const dfltExt = ".txt"

const (
	dfltMinSize = cos.KiB
	dfltMaxSize = cos.MiB

	maxNumFiles     = 10_000
	DynamicNumFiles = math.MaxInt
)

type Arch struct {
	Mime    string // archive.ExtTar|ExtTgz|ExtTarGz|ExtZip|ExtTarLz4
	Prefix  string // optional prefix inside archive (e.g., "trunk-", "a/b/c/trunk-")
	MinSize int64  // min file size
	MaxSize int64  // max file size
	Seed    uint64 // optional seed for random works (otherwise, using mono-time)
	Num     int    // files per shard
	//
	// advanced options - each and every one can be omitted
	//
	Names     []string   // explicit in-archive names
	RecExt    string     // default ".txt"; see also extended comment below
	TarFormat tar.Format // to override cmn/archive default
	MaxLayers int        // upper bound for RandDir; default 5
	RandNames bool       // generate numeric-random names
	RandDir   bool       // add random dir layers
	//
	// dynamic sizing
	//
	objSize int64
}

//////////
// Arch //
//////////

// validate Arch fields, normalize Mime, and fills defaults
func (a *Arch) Init(objSize int64) error {
	if a.objSize != 0 || a.Num == DynamicNumFiles {
		return errors.New("readers.Arch already initialized")
	}
	// dynamic sizing (pickSize())
	a.objSize = objSize

	if a.Num == 0 && a.objSize == 0 {
		return errors.New("invalid readers.Arch: both Arch.Num and object size are zero")
	}
	if a.Num < 0 || a.Num > maxNumFiles {
		return fmt.Errorf("invalid Arch.Num (%d)", a.Num)
	}
	if a.MinSize < 0 {
		return fmt.Errorf("Arch.MinSize must be non-negative (got %d)", a.MinSize)
	}
	if a.MaxSize != 0 && a.MaxSize < a.MinSize {
		return fmt.Errorf("Arch.MaxSize (%d) must be >= MinSize (%d)", a.MaxSize, a.MinSize)
	}
	if l := len(a.Names); l > 0 {
		if a.Num == 0 {
			a.Num = l
		}
		if l != a.Num {
			return fmt.Errorf("Arch.Names length (%d) must match Num (%d)", len(a.Names), a.Num)
		}
	}

	m, err := archive.Mime(a.Mime, "")
	if err != nil {
		return fmt.Errorf("Arch.Mime (%q) is invalid: %v", a.Mime, err)
	}
	a.Mime = m

	// set defaults
	if a.MinSize == 0 && a.MaxSize == 0 {
		a.MinSize, a.MaxSize = dfltMinSize, dfltMaxSize
	} else if a.MaxSize == 0 {
		a.MaxSize = a.MinSize
	}

	// clamp to object size
	if objSize > 0 {
		// clamp file sizes to object size
		a.MaxSize = min(objSize, a.MaxSize)
		a.MinSize = min(a.MaxSize, a.MinSize)

		if int64(a.Num)*a.MinSize > objSize {
			return fmt.Errorf("object size (%d) cannot be less than Arch.MinSize(%d) * Arch.Num(%d)",
				objSize, a.MinSize, a.Num)
		}
	}

	a.Num = cos.NonZero(a.Num, DynamicNumFiles)

	if a.Seed == 0 {
		a.Seed = uint64(mono.NanoTime())
	}
	switch {
	case a.RecExt == "":
		a.RecExt = dfltExt
	case a.RecExt == ".":
		return fmt.Errorf("Arch.RecExt (%q) is invalid", a.RecExt)
	case a.RecExt[0] != '.':
		a.RecExt = "." + a.RecExt
	}

	return nil
}

func (a *Arch) randInRange(idx uint64) int64 {
	span := uint64((a.MaxSize - a.MinSize) + 1) // inclusive
	return a.MinSize + int64(xoshiro256.Hash(a.Seed+idx)%span)
}

// size selection:
// - Max > Min: uniform in [Min,Max]
// - otherwise: fixed size
func (a *Arch) pickSize(i int) int64 {
	if a.MaxSize > a.MinSize {
		return a.randInRange(uint64(i))
	}
	return a.MinSize
}

// decimal-like random name
func (a *Arch) randomDigitsName(i int) string {
	r := xoshiro256.Hash(a.Seed + uint64(i))
	n := (r >> 33) // top 31 bits
	return strconv.FormatUint(n, 10)
}

// 5-letter [a-z] dir name
func (a *Arch) randomLayer(salt uint64) string {
	v := xoshiro256.Hash(a.Seed + salt)
	var b [5]byte
	for i := range 5 {
		v = xoshiro256.Hash(v + uint64(i+1))
		b[i] = byte('a' + (v % 26))
	}
	return string(b[:])
}

// build the in-archive path using Arch knobs

// NOTE in re: in-archive filename extensions ===================================
// For WebDataset-style record grouping (where each sample has multiple
// files with different extensions grouped together, e.g., sample000.jpg +
// sample000.json + sample000.txt), use explicit Names with appropriate extensions.
// The legacy tools/tarch supported this via nested loops over RecExts, but the
// current architecture with unique per-file indexing in buildPath() doesn't
// support automatic grouping semantics. ========================================

func (a *Arch) buildPath(i int) string {
	if len(a.Names) > 0 {
		// caller-provided names (but avoid absolute paths inside arch)
		name := strings.TrimLeft(a.Names[i], "/")
		if a.Prefix != "" {
			name = path.Join(a.Prefix, name)
		}
		return name
	}

	var base string
	if a.RandNames {
		base = a.randomDigitsName(i)
	} else {
		base = fmt.Sprintf("obj-%06d", i)
	}
	base += a.RecExt

	name := base
	if a.RandDir {
		const randomLayerSalt = 17 // prime for poor man's random below
		maxLayers := a.MaxLayers
		if maxLayers <= 0 {
			maxLayers = 5
		}
		layers := int(xoshiro256.Hash(a.Seed+uint64(i)) % uint64(maxLayers))
		for d := range layers {
			name = path.Join(a.randomLayer(uint64(i*randomLayerSalt+d+1)), name)
		}
	}
	if a.Prefix != "" {
		name = path.Join(a.Prefix, name)
	}
	return strings.TrimLeft(name, "/")
}

func (a *Arch) write(w io.Writer, cksumType string) (*cos.CksumHash, error) {
	debug.Assertf(a != nil && a.MinSize >= 0 && a.MaxSize >= a.MinSize, "invalid readers.Arch %+v", a)

	var cksumHS *cos.CksumHashSize
	if cksumType != cos.ChecksumNone {
		cksumHS = &cos.CksumHashSize{}
		cksumHS.Init(cksumType)
	}
	var opts *archive.Opts
	if a.TarFormat != 0 { // tar.FormatUnknown is the zero value
		opts = &archive.Opts{TarFormat: a.TarFormat}
	}
	aw := archive.NewWriter(a.Mime, w, cksumHS, opts)

	var (
		total int64
		tr    = newTruffle()
		now   = mono.NanoTime()
	)
	for i := range a.Num {
		sz := a.pickSize(i)
		if a.objSize > 0 && i > 0 && total+sz > a.objSize {
			break
		}
		name := a.buildPath(i)
		oah := cos.SimpleOAH{Size: sz, Atime: now}
		lim := &rrLimited{random: tr, size: sz, off: 0}
		if err := aw.Write(name, oah, lim); err != nil {
			return nil, err
		}
		total += sz
	}
	if err := aw.Fini(); err != nil {
		return nil, err
	}

	if cksumType != cos.ChecksumNone {
		cksumHS.Finalize()
		return &cksumHS.CksumHash, nil
	}
	return nil, nil
}
