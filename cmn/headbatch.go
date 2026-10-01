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
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Head-batch (hdb): POST /v1/objects (no bucket in the URL) with a packed body.
// One request may carry objects from different buckets. See apc.HdbResp for the response.
//
// Packed layout (strings are length-prefixed):
//
//	HdbReq: | nbck int32 | Bck x nbck | n int32 | HdbIn x n |
//	Bck:    | name str | provider str | ns-uuid str | ns-name str |
//	HdbIn:  | bidx int32 | name str | size int64 | version str | cksum-type str | cksum-value str | nmd int32 | (key str, value str) x nmd |

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
var (
	_ cos.OAH      = (*HdbIn)(nil)
	_ cos.Packer   = (*HdbIn)(nil)
	_ cos.Unpacker = (*HdbIn)(nil)
	_ cos.Packer   = (*HdbReq)(nil)
	_ cos.Unpacker = (*HdbReq)(nil)
)

// the sender calls it before Pack while the receiver calls it after Unpack
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
	p := cos.NewPacker(nil, req.PackedSize())
	p.WriteAny(req)
	return p.Bytes()
}

func (req *HdbReq) PackedSize() int {
	n := cos.SizeofI32 + cos.SizeofI32 // nbck, n
	for i := range req.Bcks {
		n += hdbBckSize(&req.Bcks[i])
	}
	for i := range req.In {
		n += req.In[i].PackedSize()
	}
	return n
}

func (req *HdbReq) Pack(p *cos.BytePack) {
	p.WriteInt32(int32(len(req.Bcks)))
	for i := range req.Bcks {
		packHdbBck(p, &req.Bcks[i])
	}
	p.WriteInt32(int32(len(req.In)))
	for i := range req.In {
		p.WriteAny(&req.In[i])
	}
}

func (req *HdbReq) Unpack(u *cos.ByteUnpack) error {
	nbck, err := u.ReadInt32()
	if err != nil {
		return fmt.Errorf("%s: %w", apc.HdbTag, err)
	}
	if nbck <= 0 || nbck > apc.HdbSizeMax {
		return fmt.Errorf("%s: invalid number of buckets %d, expecting [1, %d]", apc.HdbTag, nbck, apc.HdbSizeMax)
	}
	req.Bcks = make([]Bck, nbck)
	for i := range req.Bcks {
		if err := unpackHdbBck(u, &req.Bcks[i]); err != nil {
			return fmt.Errorf("%s: bucket %d: %w", apc.HdbTag, i, err)
		}
	}
	n, err := u.ReadInt32()
	if err != nil {
		return fmt.Errorf("%s: %w", apc.HdbTag, err)
	}
	if n <= 0 || n > apc.HdbSizeMax {
		return fmt.Errorf("%s: invalid number of items %d, expecting [1, %d]", apc.HdbTag, n, apc.HdbSizeMax)
	}
	req.In = make([]HdbIn, n)
	for i := range req.In {
		if err := req.In[i].Unpack(u); err != nil {
			return fmt.Errorf("%s: item %d: %w", apc.HdbTag, i, err)
		}
	}
	if u.Len() != 0 {
		return fmt.Errorf("%s: %d trailing bytes", apc.HdbTag, u.Len())
	}
	return nil
}

func hdbBckSize(b *Bck) int {
	return cos.PackedStrLen(b.Name) + cos.PackedStrLen(b.Provider) + cos.PackedStrLen(b.Ns.UUID) + cos.PackedStrLen(b.Ns.Name)
}

func packHdbBck(p *cos.BytePack, b *Bck) {
	p.WriteString(b.Name)
	p.WriteString(b.Provider)
	p.WriteString(b.Ns.UUID)
	p.WriteString(b.Ns.Name)
}

func unpackHdbBck(u *cos.ByteUnpack, b *Bck) (err error) {
	if b.Name, err = u.ReadString(); err != nil {
		return err
	}
	if b.Provider, err = u.ReadString(); err != nil {
		return err
	}
	if b.Ns.UUID, err = u.ReadString(); err != nil {
		return err
	}
	b.Ns.Name, err = u.ReadString()
	return err
}

func (in *HdbIn) PackedSize() int {
	ty, val := in.cksum()
	n := cos.SizeofI32 +
		cos.PackedStrLen(in.Name) +
		cos.SizeofI64 +
		cos.PackedStrLen(in.Version()) +
		cos.PackedStrLen(ty) +
		cos.PackedStrLen(val) +
		cos.SizeofI32
	for k, v := range in.CustomMD {
		n += cos.PackedStrLen(k) + cos.PackedStrLen(v)
	}
	return n
}

func (in *HdbIn) Pack(p *cos.BytePack) {
	p.WriteInt32(in.Bidx)
	p.WriteString(in.Name)
	p.WriteInt64(in.Size)
	p.WriteString(in.Version())

	ty, val := in.cksum()
	p.WriteString(ty)
	p.WriteString(val)

	p.WriteInt32(int32(len(in.CustomMD)))
	for k, v := range in.CustomMD {
		p.WriteString(k)
		p.WriteString(v)
	}
}

func (in *HdbIn) Unpack(u *cos.ByteUnpack) (err error) {
	var ver, ty, val string
	*in = HdbIn{}
	if in.Bidx, err = u.ReadInt32(); err != nil {
		return err
	}
	if in.Name, err = u.ReadString(); err != nil {
		return err
	}
	if in.Size, err = u.ReadInt64(); err != nil {
		return err
	}
	if ver, err = u.ReadString(); err != nil {
		return err
	}
	in.SetVersion(ver)
	if ty, err = u.ReadString(); err != nil {
		return err
	}
	if val, err = u.ReadString(); err != nil {
		return err
	}
	if ty != "" {
		in.Cksum = cos.NewCksum(ty, val)
	}

	nmd, err := u.ReadInt32()
	if err != nil {
		return err
	}
	// each entry has at least two length markers
	if nmd < 0 || int(nmd) > u.Len()/(2*cos.SizeofLen) {
		return fmt.Errorf("invalid number of custom metadata entries %d", nmd)
	}
	if nmd == 0 {
		return nil
	}
	in.CustomMD = make(cos.StrKVs, nmd)
	for range nmd {
		var k, v string
		if k, err = u.ReadString(); err != nil {
			return err
		}
		if v, err = u.ReadString(); err != nil {
			return err
		}
		in.CustomMD[k] = v
	}
	return nil
}

func (in *HdbIn) cksum() (ty, val string) {
	if !cos.NoneC(in.Cksum) {
		ty, val = in.Cksum.Get()
	}
	return ty, val
}
