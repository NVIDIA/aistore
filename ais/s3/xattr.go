// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

const mptXattrID = "user.ais.s3-multipart"

const iniCapParts = 8

func OffsetSorted(lom *core.LOM, partNum int64) (off, size int64, status int, err error) {
	var mpt *mpt
	if mpt, err = loadMptXattr(lom.FQN); err != nil {
		return
	}
	if mpt == nil {
		return -1, 0, http.StatusNotFound, fmt.Errorf("%s: multipart state not found", lom)
	}

	off, size, err = mpt._offSorted(lom.Cname(), partNum)
	return
}

func loadMptXattr(fqn string) (out *mpt, err error) {
	b, err := fs.GetXattr(fqn, mptXattrID)
	if err == nil {
		out = &mpt{}
		err = out.unpack(b)
		return
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}

func storeMptXattr(fqn string, mpt *mpt) (err error) {
	sort.Slice(mpt.parts, func(i, j int) bool {
		return mpt.parts[i].Num < mpt.parts[j].Num
	})
	b := mpt.pack()
	return fs.SetXattr(fqn, mptXattrID, b)
}

/////////
// mpt //
/////////

func (mpt *mpt) _offSorted(name string, num int64) (off, size int64, err error) {
	var prev = int64(-1)
	for _, part := range mpt.parts {
		debug.Assert(part.Num > prev) // must ascend
		if part.Num == num {
			size = part.Size
			return
		}
		off += part.Size
		prev = part.Num
	}
	return 0, 0, fmt.Errorf("invalid part number %d (%s has %d)", num, name, prev)
}

func (mpt *mpt) packedSize() (size int) {
	for _, part := range mpt.parts {
		size += cos.SizeofI64 // num
		size += cos.SizeofLen + len(part.MD5)
		size += cos.SizeofI64 // part.Size
	}
	return
}

func (mpt *mpt) pack() []byte {
	packer := cos.NewPacker(nil, mpt.packedSize())
	for _, part := range mpt.parts {
		packer.WriteInt64(part.Num)
		packer.WriteString(part.MD5)
		packer.WriteInt64(part.Size)
	}
	return packer.Bytes()
}

func (mpt *mpt) unpack(b []byte) (err error) {
	unpacker := cos.NewUnpacker(b)
	debug.Assert(mpt.parts == nil)
	mpt.parts = make([]*MptPart, 0, iniCapParts)
	for unpacker.Len() > 0 {
		part := &MptPart{}
		if part.Num, err = unpacker.ReadInt64(); err != nil {
			break
		}
		if part.MD5, err = unpacker.ReadString(); err != nil {
			break
		}
		if part.Size, err = unpacker.ReadInt64(); err != nil {
			break
		}
		mpt.parts = append(mpt.parts, part)
	}
	return
}

func (mpt *mpt) getPart(num int64) *MptPart {
	for _, part := range mpt.parts {
		if part.Num == num {
			return part
		}
	}
	return nil
}
