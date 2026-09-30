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

// Head-batch (hdb): POST /v1/objects/<bucket-name> with Content-Type: application/octet-stream.
// See apc.HdbResp for the response.
//
// Packed layout (strings are length-prefixed):
//
//	HdbReq: | n int32 | HdbIn x n |
//	HdbIn:  | name str | size int64 | version str | cksum-type str | cksum-value str | nmd int32 | (key str, value str) x nmd |

// The sender must fall back to per-object HeadObjT2T when peers don't support it.
var ErrHdbUnsupported = errors.New("head-batch: unsupported by peer")

func IsErrHdbUnsupported(err error) bool { return errors.Is(err, ErrHdbUnsupported) }

type (
	HdbIn struct {
		ObjAttrs // the source object
		Name     string
	}
	HdbReq struct {
		In []HdbIn // 1 <= len <= apc.HdbSizeMax
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

func (req *HdbReq) Validate() error {
	const tag = "head-batch"
	if len(req.In) == 0 {
		return errors.New(tag + ": empty request")
	}
	if len(req.In) > apc.HdbSizeMax {
		return fmt.Errorf(tag+": too many items (%d), expecting at most %d", len(req.In), apc.HdbSizeMax)
	}
	// no per-item name validation: intra-cluster only, names come from the sender's LOMs
	return nil
}

func (req *HdbReq) NewPack() []byte {
	p := cos.NewPacker(nil, req.PackedSize())
	p.WriteAny(req)
	return p.Bytes()
}

func (req *HdbReq) PackedSize() int {
	n := cos.SizeofI32
	for i := range req.In {
		n += req.In[i].PackedSize()
	}
	return n
}

func (req *HdbReq) Pack(p *cos.BytePack) {
	p.WriteInt32(int32(len(req.In)))
	for i := range req.In {
		p.WriteAny(&req.In[i])
	}
}

func (req *HdbReq) Unpack(u *cos.ByteUnpack) error {
	n, err := u.ReadInt32()
	if err != nil {
		return fmt.Errorf("head-batch: %w", err)
	}
	if n <= 0 || n > apc.HdbSizeMax {
		return fmt.Errorf("head-batch: invalid number of items %d, expecting [1, %d]", n, apc.HdbSizeMax)
	}
	req.In = make([]HdbIn, n)
	for i := range req.In {
		if err := req.In[i].Unpack(u); err != nil {
			return fmt.Errorf("head-batch: item %d: %w", i, err)
		}
	}
	if u.Len() != 0 {
		return fmt.Errorf("head-batch: %d trailing bytes", u.Len())
	}
	return nil
}

func (in *HdbIn) PackedSize() int {
	ty, val := in.cksum()
	n := cos.PackedStrLen(in.Name) +
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
