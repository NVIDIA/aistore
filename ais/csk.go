// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"strconv"
	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
)

const tagcsk = "csk"

type (
	clusterKey struct {
		secret  []byte
		ver     int64
		created int64
	}
)

var csk atomic.Value

// interface guard
var (
	_ cos.Packer   = (*clusterKey)(nil)
	_ cos.Unpacker = (*clusterKey)(nil)
	_ revs         = (*clusterKey)(nil)
)

func cskload() (k *clusterKey) { return csk.Load().(*clusterKey) }
func cskstore(k *clusterKey)   { csk.Store(k) }
func cskreset()                { cskstore(&clusterKey{}) }

// primary only;
// version is monotonically increasing and is loosely tied to smap version:
// the latter is strictly guarded by primary and  can therefore, be relied
// upon in re: false-positive downgrades
func cskgen(smapVer int64) {
	ok := cskload()
	nk := &clusterKey{
		secret:  []byte(cos.CryptoRandS(16)),
		ver:     max(smapVer, ok.ver+1),
		created: mono.NanoTime(),
	}
	cskstore(nk)
}

//
// as byte-packer
//

func (k *clusterKey) PackedSize() int {
	return cos.SizeofI64 + cos.SizeofI64 + cos.PackedBytesLen(k.secret)
}

func (k *clusterKey) Pack(packer *cos.BytePack) {
	packer.WriteInt64(k.ver)
	packer.WriteInt64(k.created)
	packer.WriteBytes(k.secret)
}

func (k *clusterKey) Unpack(unpacker *cos.ByteUnpack) (err error) {
	if k.ver, err = unpacker.ReadInt64(); err != nil {
		return err
	}
	if k.created, err = unpacker.ReadInt64(); err != nil {
		return err
	}
	k.secret, err = unpacker.ReadBytes()
	return err
}

//
// as metasync `revs`
//

func (*clusterKey) tag() string       { return revsCSKTag }
func (k *clusterKey) version() int64  { return k.ver }
func (*clusterKey) uuid() string      { return "" }
func (k *clusterKey) jit(*proxy) revs { return k }
func (*clusterKey) sgl() *memsys.SGL  { return nil }

func (k *clusterKey) marshal() []byte {
	size := k.PackedSize()
	packer := cos.NewPacker(make([]byte, size), size)
	packer.WriteAny(k)
	return packer.Bytes()
}

func (k *clusterKey) String() string {
	return tagcsk + " v" + strconv.FormatInt(k.ver, 10)
}
