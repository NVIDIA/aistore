// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/binpack"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Head-batch (hdb): POST /v1/objects (no bucket in the URL) with a packed body.
// One request may carry objects from different buckets. See apc.HdbResp for the response.
//
// Packed layout (str: see cmn/binpack, attrs: see ObjAttrs.InsAttrs):
//
//	HdbReq: | nbck uint16 | Bck x nbck | n uint16 | HdbIn x n |
//	Bck:    | name str | provider str | ns-uuid str | ns-name str |
//	HdbIn:  | bidx uint16 | name str | attrs |

// The sender must fall back to per-object HeadObjT2T when the peer does not support it.
var ErrHdbUnsupported = errors.New(apc.HdbTag + ": unsupported by peer")

func IsErrHdbUnsupported(err error) bool { return errors.Is(err, ErrHdbUnsupported) }

type (
	// one object: the sender's attributes to compare with the receiver's copy
	HdbIn struct {
		ObjAttrs // the source object
		Name     string
		Bidx     int32 // HdbReq.Bcks[Bidx] is the bucket
	}
	// one request to one peer (target)
	HdbReq struct {
		Bcks []Bck   // 1 <= len <= len(In)
		In   []HdbIn // 1 <= len <= apc.HdbSizeMax
	}
)

// interface guard
var _ cos.OAH = (*HdbIn)(nil)

// the sender calls it before NewPack while the receiver calls it after Unpack
func (req *HdbReq) Validate() error {
	if len(req.In) == 0 {
		return errors.New(apc.HdbTag + ": empty request")
	}
	if len(req.In) > apc.HdbSizeMax {
		return fmt.Errorf("%s: too many items (%d), expecting at most %d", apc.HdbTag, len(req.In), apc.HdbSizeMax)
	}
	if len(req.Bcks) == 0 || len(req.Bcks) > len(req.In) {
		return fmt.Errorf("%s: invalid number of buckets %d, expecting [1, %d]", apc.HdbTag, len(req.Bcks), len(req.In))
	}
	for i := range req.In {
		if bidx := req.In[i].Bidx; bidx < 0 || int(bidx) >= len(req.Bcks) {
			return fmt.Errorf("%s: item %d: bucket index %d out of range [0, %d)", apc.HdbTag, i, bidx, len(req.Bcks))
		}
	}
	// no per-item name validation: intra-cluster only, names come from the sender's LOMs
	return nil
}

// add the bucket unless already present
// return its index in req.Bcks
func (req *HdbReq) AddBck(bck *Bck) (bidx int32) {
	for i := len(req.Bcks) - 1; i >= 0; i-- {
		if req.Bcks[i].Equal(bck) {
			return int32(i)
		}
	}
	req.Bcks = append(req.Bcks, Bck{Name: bck.Name, Provider: bck.Provider, Ns: bck.Ns})
	return int32(len(req.Bcks) - 1)
}

func (req *HdbReq) NewPack() []byte {
	b := make([]byte, req.PackedSize())
	req.PackTo(b)
	return b
}

// PackTo packs the request into b
// the length of b must be equal to PackedSize
func (req *HdbReq) PackTo(b []byte) {
	off := binpack.InsUint16(0, b, len(req.Bcks))
	for i := range req.Bcks {
		off = packHdbBck(off, b, &req.Bcks[i])
	}
	off = binpack.InsUint16(off, b, len(req.In))
	for i := range req.In {
		in := &req.In[i]
		off = binpack.InsUint16(off, b, int(in.Bidx))
		off = binpack.InsString(off, b, in.Name)
		off = in.InsAttrs(off, b)
	}
	debug.Assert(off == len(b), off, " vs ", len(b))
}

func (req *HdbReq) PackedSize() int {
	n := cos.SizeofI16 + cos.SizeofI16 // nbck, n
	for i := range req.Bcks {
		n += hdbBckSize(&req.Bcks[i])
	}
	for i := range req.In {
		in := &req.In[i]
		n += cos.SizeofI16 + binpack.StrSize(in.Name) + in.PackedAttrsSize()
	}
	return n
}

func (req *HdbReq) Unpack(body []byte) error {
	d := binpack.NewDecoder(body)
	nbck := d.Uint16()
	if d.Bad() {
		return fmt.Errorf("%s: truncated at offset %d", apc.HdbTag, d.Off())
	}
	if nbck == 0 || nbck > apc.HdbSizeMax {
		return fmt.Errorf("%s: invalid number of buckets %d, expecting [1, %d]", apc.HdbTag, nbck, apc.HdbSizeMax)
	}
	req.Bcks = make([]Bck, nbck)
	for i := range req.Bcks {
		unpackHdbBck(&d, &req.Bcks[i])
	}
	n := d.Uint16()
	if d.Bad() {
		return fmt.Errorf("%s: truncated at offset %d", apc.HdbTag, d.Off())
	}
	if n == 0 || n > apc.HdbSizeMax {
		return fmt.Errorf("%s: invalid number of items %d, expecting [1, %d]", apc.HdbTag, n, apc.HdbSizeMax)
	}
	req.In = make([]HdbIn, n)
	for i := range req.In {
		in := &req.In[i]
		in.Bidx = int32(d.Uint16())
		in.Name = d.Str()
		in.ExtAttrs(&d, nil /*prev*/)
		if d.Bad() {
			return fmt.Errorf("%s: item %d: truncated at offset %d", apc.HdbTag, i, d.Off())
		}
	}
	if l := d.Len(); l != 0 {
		return fmt.Errorf("%s: %d trailing bytes", apc.HdbTag, l)
	}
	return nil
}

func hdbBckSize(b *Bck) int {
	return binpack.StrSize(b.Name) + binpack.StrSize(b.Provider) + binpack.StrSize(b.Ns.UUID) + binpack.StrSize(b.Ns.Name)
}

func packHdbBck(off int, to []byte, b *Bck) int {
	off = binpack.InsString(off, to, b.Name)
	off = binpack.InsString(off, to, b.Provider)
	off = binpack.InsString(off, to, b.Ns.UUID)
	return binpack.InsString(off, to, b.Ns.Name)
}

func unpackHdbBck(d *binpack.Decoder, b *Bck) {
	b.Name = d.Str()
	b.Provider = d.Str()
	b.Ns.UUID = d.Str()
	b.Ns.Name = d.Str()
}
