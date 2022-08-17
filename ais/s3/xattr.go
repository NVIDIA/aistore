// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

const uploadXattrID = "user.ais.s3-multipart"

const iniCapParts = 8

func LoadMptXattr(fqn string) (upload *mpt, err error) {
	b, err := fs.GetXattr(fqn, uploadXattrID)
	if err == nil {
		upload = &mpt{}
		err = upload.unpack(b)
		return
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}

func storeMptXattr(fqn string, upload *mpt) (err error) {
	b := upload.pack()
	return fs.SetXattr(fqn, uploadXattrID, b)
}

/////////
// mpt //
/////////

func (mpt *mpt) packedSize() (size int) {
	size = cos.SizeofLen + len(mpt.objName)
	for _, part := range mpt.parts {
		size += cos.SizeofI64 // num
		size += cos.SizeofLen + len(part.MD5)
		size += cos.SizeofI64 // part.Size
	}
	return
}

func (mpt *mpt) pack() []byte {
	packer := cos.NewPacker(nil, mpt.packedSize())
	packer.WriteString(mpt.objName)
	for _, part := range mpt.parts {
		packer.WriteInt64(part.Num)
		packer.WriteString(part.MD5)
		packer.WriteInt64(part.Size)
	}
	return packer.Bytes()
}

func (mpt *mpt) unpack(b []byte) (err error) {
	unpacker := cos.NewUnpacker(b)
	debug.Assert(mpt.objName == "" && mpt.parts == nil)
	if mpt.objName, err = unpacker.ReadString(); err != nil {
		return
	}
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
