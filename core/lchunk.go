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
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

const (
	umetaver = 1
)

const (
	chunkNameSepa = "."
)

// TODO: consider lower-casing member state vars

type (
	Uchunk struct {
		Path     string // (may become v2 _location_)
		CksumVal string
		Siz      int64
		Num      uint16
	}
	Ufest struct {
		ID        string
		StartTime time.Time
		Num       uint16
		CksumTyp  string
		Chunks    []Uchunk

		// runtime state
		lom    *LOM
		Size   int64
		suffix string
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

// default chunk slice capacity for new manifests
const defaultChunkCap = 16

// NewUfest creates a new chunk manifest with optional ID
// If id is empty, generates one based on current time:
//   - time-based UUIDs (e.g., "t161208-a3z") for traceability
func NewUfest(id string, lom *LOM) *Ufest {
	startTime := time.Now()
	if id == "" {
		id = cos.GenTAID(startTime)
	}
	return &Ufest{
		ID:        id,
		StartTime: startTime,
		Chunks:    make([]Uchunk, 0, defaultChunkCap),
		CksumTyp:  cos.ChecksumOneXxh,
		lom:       lom,
		suffix:    chunkNameSepa + id + chunkNameSepa,
	}
}

func (u *Ufest) Load(lom *LOM) error {
	debug.Assert(u.lom == nil)

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

	debug.Assert(u.validNums() == nil)

	u.lom = lom

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

// The `num` here is Uchunk.Num; notes:
// - chunk #1 stays on object's mountpath
// - other chunks get HRW distributed
func (u *Ufest) ChunkName(num int) (string, error) {
	debug.Assert(u.lom != nil)
	if num <= 0 {
		return "", fmt.Errorf("%s: invalid chunk number (%d)", _utag(u.lom.Cname()), num)
	}
	var (
		contentResolver = fs.CSM.Resolver(fs.ObjChunkType)
		suffix          = u.suffix + fmt.Sprintf("%04d", num)
		chname          = contentResolver.GenUniqueFQN(u.lom.ObjName, suffix)
	)
	if num == 1 {
		return u.lom.Mountpath().MakePathFQN(u.lom.Bucket(), fs.ObjChunkType, chname), nil
	}
	mi, _ /*digest*/, err := fs.Hrw(cos.UnsafeB(chname))
	return mi.MakePathFQN(u.lom.Bucket(), fs.ObjChunkType, chname), err
}

func (u *Ufest) validNums() error {
	if u.Num == 0 {
		return errors.New("no chunks to validate")
	}
	for i, c := range u.Chunks {
		if c.Num <= 0 || c.Num > u.Num {
			return fmt.Errorf("chunk %d has invalid part number [%d, %d]", i, c.Num, u.Num)
		}
		for j := range i {
			if u.Chunks[j].Num == c.Num {
				return fmt.Errorf("duplicate part number: [%d, %d, %d]", c.Num, i, j)
			}
		}
	}
	return nil
}

func (u *Ufest) Store(lom *LOM) error {
	// validate
	var total int64
	if u.Num == 0 || int(u.Num) != len(u.Chunks) {
		return fmt.Errorf("store %s: num %d vs %d", _utag(lom.Cname()), u.Num, len(u.Chunks))
	}
	lsize := lom.Lsize(true)
	if u.Size != 0 && u.Size != lsize {
		return fmt.Errorf("%s store size: %d vs %d", _utag(lom.Cname()), u.Size, lsize)
	}
	for i := range u.Num {
		total += u.Chunks[i].Siz
	}
	if total != lsize {
		return fmt.Errorf("store %s: total size mismatch (%d vs %d)", _utag(lom.Cname()), total, lsize)
	}

	// sort
	sort.Slice(u.Chunks, func(i, j int) bool { return u.Chunks[i].Num < u.Chunks[j].Num })

	if err := u.validNums(); err != nil {
		return fmt.Errorf("%s: failed to store, err: %v", _utag(lom.Cname()), err)
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
		return fmt.Errorf("store %s: too large (%d > %d max)", _utag(lom.Cname()), sgl.Len(), xattrChunkMax)
	}

	// write
	if err := lom.setXchunk(sgl.Bytes()); err != nil {
		return err
	}
	lom.md.lid.setlmfl(lmflChunk) // TODO -- FIXME: persist
	return nil
}

func (u *Ufest) pack(w io.Writer) {
	var (
		b64 [cos.SizeofI64]byte
		b16 [cos.SizeofI16]byte
	)

	// meta-version
	w.Write([]byte{umetaver})

	// ID
	_packStr(w, u.ID)

	// StartTime (Unix timestamp)
	binary.BigEndian.PutUint64(b64[:], uint64(u.StartTime.Unix()))
	w.Write(b64[:])

	// number of chunks
	binary.BigEndian.PutUint16(b16[:], u.Num)
	w.Write(b16[:])

	// checksum type
	_packStr(w, u.CksumTyp)

	// chunks
	for _, c := range u.Chunks {
		// chunk size
		binary.BigEndian.PutUint64(b64[:], uint64(c.Siz))
		w.Write(b64[:])

		// chunk num
		binary.BigEndian.PutUint16(b16[:], c.Num)
		w.Write(b16[:])

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

	// ID
	if u.ID, offset, err = _unpackStr(data, offset); err != nil {
		return err
	}

	// StartTime
	if len(data) < offset+cos.SizeofI64 {
		return errors.New(tooShort)
	}
	timestamp := int64(binary.BigEndian.Uint64(data[offset:]))
	u.StartTime = time.Unix(timestamp, 0)
	offset += cos.SizeofI64

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

		// chunk num
		if len(data) < offset+cos.SizeofI16 {
			return errors.New(tooShort)
		}
		c.Num = binary.BigEndian.Uint16(data[offset:])
		offset += cos.SizeofI16

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
