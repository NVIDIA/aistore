// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

const (
	umetaver = 1
)

type (
	Uchunk struct {
		Siz      int64
		Path     string // (may become v2 _location_)
		CksumVal string
	}
	Ufest struct {
		Num      uint16
		CksumTyp string
		Chunks   []Uchunk
		// runtime state
		Size int64
	}
)

// on-disk xattr
const (
	xattrChunk = "user.ais.chunk"

	xattrChunkDflt = memsys.DefaultBufSize
	xattrChunkMax  = memsys.DefaultBuf2Size // maximum 64K
)

const (
	utag     = "chunk-manifest"
	itag     = "invalid " + utag
	tooShort = "failed to unpack: too short"
)

func (u *Ufest) Load(lom *LOM) error {
	cbuf, cslab := g.pmm.AllocSize(xattrChunkMax)
	defer cslab.Free(cbuf)

	data, err := lom.getXchunk(cbuf)
	if err != nil {
		return _ucerr(lom.Cname(), err)
	}

	// validate and strip/remove the trailing checksum
	checksumOffset := len(data) - cos.SizeXXHash64
	expectedChecksum := binary.BigEndian.Uint64(data[checksumOffset:])
	compressedData := data[:checksumOffset]
	actualChecksum := onexxh.Checksum64S(compressedData, cos.MLCG32)
	if expectedChecksum != actualChecksum {
		return cos.NewErrMetaCksum(expectedChecksum, actualChecksum, utag)
	}

	// decompress into a 2nd buffer
	dbuf, dslab := g.pmm.AllocSize(xattrChunkMax)
	err = u._load(lom, compressedData, dbuf)
	dslab.Free(dbuf)
	return err
}

func (u *Ufest) _load(lom *LOM, compressedData, buf []byte) error {
	data := buf[:0]
	zr := lz4.NewReader(bytes.NewReader(compressedData))

	for {
		if len(data) == cap(data) {
			return fmt.Errorf("%s too large", utag)
		}
		n, err := zr.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("%s decompression failed: %v", utag, err)
		}
	}

	if err := u.unpack(data); err != nil {
		return _ucerr(lom.Cname(), err)
	}
	if u.Size != lom.Lsize(true) {
		return fmt.Errorf("%s load size mismatch: %d vs %d", _utag(lom.Cname()), u.Size, lom.Lsize(true))
	}
	return nil
}

func _ucerr(cname string, err error) error {
	if cos.IsErrXattrNotFound(err) {
		return cmn.NewErrLmetaNotFound(_utag(cname), err)
	}
	return os.NewSyscallError(getxattr, fmt.Errorf("%s, err: %w", _utag(cname), err))
}

func _utag(cname string) string { return itag + "[" + cname + "]" }

func (u *Ufest) Store(lom *LOM) error {
	// validate
	var total int64
	if u.Num == 0 || int(u.Num) != len(u.Chunks) {
		return fmt.Errorf("%s: num %d vs %d", _utag(lom.Cname()), u.Num, len(u.Chunks))
	}
	lsize := lom.Lsize(true)
	if u.Size != 0 && u.Size != lsize {
		return fmt.Errorf("%s store size: %d vs %d", _utag(lom.Cname()), u.Size, lsize)
	}
	for i := range u.Num {
		total += u.Chunks[i].Siz
	}
	if total != lsize {
		return fmt.Errorf("%s: total size mismatch (%d vs %d)", _utag(lom.Cname()), total, lsize)
	}

	// pack
	sgl := g.pmm.NewSGL(xattrChunkDflt)
	defer sgl.Free()

	zw := lz4.NewWriter(sgl)
	u.pack(zw)
	zw.Close()

	// compute and write trailing checksum
	data := sgl.Bytes()
	h := onexxh.Checksum64S(data, cos.MLCG32)
	var checksumBuf [cos.SizeXXHash64]byte
	binary.BigEndian.PutUint64(checksumBuf[:], h)
	sgl.Write(checksumBuf[:])

	if sgl.Len() > xattrChunkMax {
		return fmt.Errorf("%s: too large (%d > %d max)", _utag(lom.Cname()), sgl.Len(), xattrChunkMax)
	}

	// write
	if err := lom.setXchunk(sgl.Bytes()); err != nil {
		return err
	}
	lom.md.lid.setlmfl(lmflChunk) // TODO -- FIXME: persist
	return nil
}

func (u *Ufest) pack(w io.Writer) {
	// meta-version
	w.Write([]byte{umetaver})

	// number of chunks
	var buf [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(buf[:], u.Num)
	w.Write(buf[:])

	// checksum type
	_packStr(w, u.CksumTyp)

	// chunks
	for _, c := range u.Chunks {
		// chunk size
		var sizeBuf [cos.SizeofI64]byte
		binary.BigEndian.PutUint64(sizeBuf[:], uint64(c.Siz))
		w.Write(sizeBuf[:])

		// chunk path
		_packStr(w, c.Path)

		// chunk checksum
		_packStr(w, c.CksumVal)
	}
}

func _packStr(w io.Writer, s string) {
	var lenBuf [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(s)))
	w.Write(lenBuf[:])
	w.Write(cos.UnsafeB(s))
}

func (u *Ufest) unpack(data []byte) (err error) {
	if len(data) < 1 {
		return errors.New(tooShort)
	}

	var offset int

	// meta-version
	metaver := data[offset]
	offset++
	if metaver != umetaver {
		return fmt.Errorf("unsupported %s meta-version %d (expecting %d)", utag, metaver, umetaver)
	}

	// number of chunks
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	u.Num = binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16

	// checksum type
	if u.CksumTyp, offset, err = _unpackStr(data, offset); err != nil {
		return err
	}

	// Read chunks
	u.Chunks = make([]Uchunk, u.Num)
	u.Size = 0
	for i := range u.Num {
		c := &u.Chunks[i]

		// chunk size
		if len(data) < offset+cos.SizeofI64 {
			return errors.New(tooShort)
		}
		c.Siz = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += cos.SizeofI64
		u.Size += c.Siz
		// chunk path
		if c.Path, offset, err = _unpackStr(data, offset); err != nil {
			return err
		}
		// chunk checksum
		if c.CksumVal, offset, err = _unpackStr(data, offset); err != nil {
			return err
		}
	}
	return nil
}

func _unpackStr(data []byte, offset int) (string, int, error) {
	if len(data) < offset+cos.SizeofI16 {
		return "", offset, errors.New(tooShort)
	}

	l := int(binary.BigEndian.Uint16(data[offset:]))
	offset += cos.SizeofI16
	if len(data) < offset+l {
		return "", offset, errors.New(tooShort)
	}
	str := string(data[offset : offset+l])
	offset += l
	return str, offset, nil
}

//
// additional lom
//

func (lom *LOM) getXchunk(buf []byte) ([]byte, error) {
	return fs.GetXattrBuf(lom.FQN, xattrChunk, buf)
}

func (lom *LOM) setXchunk(data []byte) error {
	return fs.SetXattr(lom.FQN, xattrChunk, data)
}
