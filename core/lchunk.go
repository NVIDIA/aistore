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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

const (
	umetaver = 1
)

const (
	chunkNameSepa   = "."
	defaultChunkCap = 16
)

type (
	Uchunk struct {
		Siz      int64
		Path     string // (may become v2 _location_)
		CksumVal string
		Num      uint16 // chunk/part number
		MD5      string // S3-specific MD5 hash
	}
	Ufest struct {
		ID        string    // upload/manifest ID
		StartTime time.Time // creation time
		Num       uint16
		CksumTyp  string
		Chunks    []Uchunk
		Metadata  map[string]string // remote object metadata

		// runtime state
		Lom    *LOM
		Size   int64
		suffix string
		mu     sync.Mutex
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
		Lom:       lom,
		suffix:    chunkNameSepa + id + chunkNameSepa,
	}
}

func (u *Ufest) Lock()   { u.mu.Lock() }
func (u *Ufest) Unlock() { u.mu.Unlock() }

func (u *Ufest) Add(c *Uchunk) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	l := len(u.Chunks)
	// append
	if l == 0 || u.Chunks[l-1].Num < c.Num {
		u.Chunks = append(u.Chunks, *c)
		u.Size += c.Siz
		u.Num = uint16(len(u.Chunks))
		return nil
	}

	idx := sort.Search(l, func(i int) bool { return u.Chunks[i].Num >= c.Num })

	// replace ("last wins")
	dup := &u.Chunks[idx]
	if idx < l && dup.Num == c.Num {
		if err := cos.RemoveFile(dup.Path); err != nil {
			return fmt.Errorf("failed to replace chunk [%d, %s]: %v", c.Num, dup.Path, err)
		}
		u.Size += c.Siz - dup.Siz
		*dup = *c
		return nil
	}

	// insert
	u.Chunks = append(u.Chunks, Uchunk{})
	copy(u.Chunks[idx+1:], u.Chunks[idx:])
	u.Chunks[idx] = *c
	u.Size += c.Siz
	u.Num = uint16(len(u.Chunks))
	return nil
}

func (u *Ufest) GetChunk(num uint16, locked bool) *Uchunk {
	if !locked {
		u.mu.Lock()
		defer u.mu.Unlock()
	}
	for i := range u.Chunks {
		if u.Chunks[i].Num == num {
			return &u.Chunks[i]
		}
	}
	return nil
}

func (u *Ufest) Abort(lom *LOM) error {
	u.mu.Lock()
	for i := range u.Chunks {
		chunk := &u.Chunks[i]
		if err := cos.RemoveFile(chunk.Path); err != nil {
			if cmn.Rom.FastV(4, cos.SmoduleCore) {
				nlog.Warningln("u.Abort: failed to remove [", u.ID, lom.Cname(), chunk.Path, err, "]")
			}
		}
	}
	u.mu.Unlock()
	return nil
}

// Generate chunk file path - The `num` here is Uchunk.Num; notes:
// - chunk #1 stays on object's mountpath
// - other chunks get HRW distributed
func (u *Ufest) ChunkName(num int) (string, error) {
	lom := u.Lom
	if lom == nil {
		return "", errors.New("nil lom")
	}
	if num <= 0 {
		return "", fmt.Errorf("%s: invalid chunk number (%d)", _utag(lom.Cname()), num)
	}
	var (
		contentResolver = fs.CSM.Resolver(fs.ObjChunkType)
		suffix          = u.suffix + fmt.Sprintf("%04d", num)
		chname          = contentResolver.GenUniqueFQN(lom.ObjName, suffix)
	)
	if num == 1 {
		return lom.Mountpath().MakePathFQN(lom.Bucket(), fs.ObjChunkType, chname), nil
	}
	mi, _ /*digest*/, err := fs.Hrw(cos.UnsafeB(chname))
	return mi.MakePathFQN(lom.Bucket(), fs.ObjChunkType, chname), err
}

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

	debug.Assert(u.validNums() == nil)

	u.Lom = lom

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
	u.mu.Lock()
	defer u.mu.Unlock()

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
		return fmt.Errorf("%s: too large (%d > %d max)", _utag(lom.Cname()), sgl.Len(), xattrChunkMax)
	}

	// write
	if err := lom.setXchunk(sgl.Bytes()); err != nil {
		return err
	}
	lom.md.lid.setlmfl(lmflChunk) // TODO -- FIXME: persist
	return nil
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

func (u *Ufest) pack(w io.Writer) {
	// meta-version
	w.Write([]byte{umetaver})

	// upload ID
	_packStr(w, u.ID)

	// start time (Unix nano)
	var timeBuf [cos.SizeofI64]byte
	binary.BigEndian.PutUint64(timeBuf[:], uint64(u.StartTime.UnixNano()))
	w.Write(timeBuf[:])

	// number of chunks
	var buf [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(buf[:], u.Num)
	w.Write(buf[:])

	// checksum type
	_packStr(w, u.CksumTyp)

	// metadata map
	var metaBuf [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(metaBuf[:], uint16(len(u.Metadata)))
	w.Write(metaBuf[:])
	for k, v := range u.Metadata {
		_packStr(w, k)
		_packStr(w, v)
	}

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

		// chunk number
		var numBuf [cos.SizeofI16]byte
		binary.BigEndian.PutUint16(numBuf[:], c.Num)
		w.Write(numBuf[:])

		// chunk MD5 (S3-specific)
		_packStr(w, c.MD5)
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

	// upload ID
	if u.ID, offset, err = _unpackStr(data, offset); err != nil {
		return err
	}

	// start time
	if len(data) < offset+cos.SizeofI64 {
		return errors.New(tooShort)
	}
	timeNano := int64(binary.BigEndian.Uint64(data[offset:]))
	u.StartTime = time.Unix(0, timeNano)
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

	// metadata map
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	metaCount := binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16

	if metaCount > 0 {
		u.Metadata = make(map[string]string, metaCount)
		for range metaCount {
			var k, v string
			if k, offset, err = _unpackStr(data, offset); err != nil {
				return err
			}
			if v, offset, err = _unpackStr(data, offset); err != nil {
				return err
			}
			u.Metadata[k] = v
		}
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

		// chunk number
		if len(data) < offset+cos.SizeofI16 {
			return errors.New(tooShort)
		}
		c.Num = binary.BigEndian.Uint16(data[offset:])
		offset += cos.SizeofI16

		// chunk MD5
		if c.MD5, offset, err = _unpackStr(data, offset); err != nil {
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
