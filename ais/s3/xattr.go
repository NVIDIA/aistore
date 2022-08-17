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

func LoadMpartXattr(fqn string) (upload *uploadInfo, err error) {
	b, err := fs.GetXattr(fqn, uploadXattrID)
	if err == nil {
		upload = &uploadInfo{}
		err = upload.unpack(b)
		return
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}

func storeMpartXattr(fqn string, upload *uploadInfo) (err error) {
	b := upload.pack()
	return fs.SetXattr(fqn, uploadXattrID, b)
}

func (upload *uploadInfo) packedSize() (size int) {
	size = cos.SizeofLen + len(upload.objName)
	for _, part := range upload.parts {
		size += cos.SizeofI64 // num
		size += cos.SizeofLen + len(part.MD5)
		size += cos.SizeofI64 // part.Size
	}
	return
}

func (upload *uploadInfo) pack() []byte {
	packer := cos.NewPacker(nil, upload.packedSize())
	packer.WriteString(upload.objName)
	for num, part := range upload.parts {
		packer.WriteInt64(num)
		packer.WriteString(part.MD5)
		packer.WriteInt64(part.Size)
	}
	return packer.Bytes()
}

func (upload *uploadInfo) unpack(b []byte) (err error) {
	unpacker := cos.NewUnpacker(b)
	debug.Assert(upload.objName == "" && upload.parts == nil)
	if upload.objName, err = unpacker.ReadString(); err != nil {
		return
	}
	upload.parts = make(map[int64]*UploadPart)
	var num int64
	for unpacker.Len() > 0 {
		if num, err = unpacker.ReadInt64(); err != nil {
			break
		}
		part := &UploadPart{}
		upload.parts[num] = part
		if part.MD5, err = unpacker.ReadString(); err != nil {
			break
		}
		if part.Size, err = unpacker.ReadInt64(); err != nil {
			break
		}
	}
	return
}
