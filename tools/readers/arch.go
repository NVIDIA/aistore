// Package readers provides implementation for common reader types
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package readers

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
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
	TarFormat tar.Format // to override cmn/archive default
	RecExts   []string   // e.g. []{".txt"}; if empty -> ".txt"
	RandNames bool       // generate numeric-random names
	RandDir   bool       // add random dir layers
	MaxLayers int        // upper bound for RandDir; default 5
}

//////////
// Arch //
//////////

func (a *Arch) randInRange(idx uint64) int64 {
	span := uint64((a.MaxSize - a.MinSize) + 1) // inclusive
	return a.MinSize + int64(xoshiro256.Hash(a.Seed+idx)%span)
}

// size selection:
// - Max > Min:  uniform in [Min,Max]
// - othwerwise: fixed size, with optional jitter for very small sizes
func (a *Arch) pickSize(i int) int64 {
	if a.MaxSize > a.MinSize {
		return a.randInRange(uint64(i))
	}
	return a.MinSize
}

// choose extension; if none, ".txt"
func (a *Arch) chooseExt() string {
	if len(a.RecExts) == 0 {
		return ".txt"
	}
	return a.RecExts[0]
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
	base += a.chooseExt()

	name := base
	if a.RandDir {
		maxLayers := a.MaxLayers
		if maxLayers <= 0 {
			maxLayers = 5
		}
		layers := int(xoshiro256.Hash(a.Seed+uint64(i)) % uint64(maxLayers))
		for d := range layers {
			name = path.Join(a.randomLayer(uint64(i*17+d+1)), name)
		}
	}
	if a.Prefix != "" {
		name = path.Join(a.Prefix, name)
	}
	return strings.TrimLeft(name, "/")
}

func (a *Arch) write(w io.Writer, cksumType string) (*cos.CksumHash, error) {
	debug.Assertf(a != nil && a.Num > 0 && a.MinSize >= 0 && a.MaxSize >= a.MinSize, "invalid readers.Arch %+v", a)

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
		tr    = newTruffle()
		now   = mono.NanoTime()
		total int64
	)
	a.Seed = cos.NonZero(a.Seed, uint64(now))
	for i := range a.Num {
		sz := a.pickSize(i)
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
	_ = total // TODO: possible future use (or remove)

	if cksumType != cos.ChecksumNone {
		cksumHS.Finalize()
		return &cksumHS.CksumHash, nil
	}
	return nil, nil
}
