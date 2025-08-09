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
	"math"
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
	chunkNameSepa = "."

	iniChunksCap = 16

	maxChunkSize = 5 * cos.GiB

	maxMetaKeys     = 1000
	maxMetaEntryLen = 4 * cos.KiB
)

const (
	completed uint16 = 1 << 0 // Ufest.Flags
)

// on-disk xattr
const (
	xattrChunk = "user.ais.chunk"

	xattrChunkDflt = memsys.DefaultBufSize
	xattrChunkMax  = memsys.DefaultBuf2Size // NOTE: 64K hard limit
)

const (
	utag     = "chunk-manifest"
	itag     = "invalid " + utag
	tooShort = "failed to unpack: too short"
)

type (
	Uchunk struct {
		Siz   int64      // size
		Path  string     // (may become v2 _remote location_)
		Cksum *cos.Cksum // nil means none; otherwise non-empty
		Num   uint16     // chunk/part number
		Flags uint16     // bit flags (future use)
		MD5   string     // S3/legacy
	}
	mdpair struct{ k, v string }
	Ufest  struct {
		ID      string    // upload/manifest ID
		Created time.Time // creation time
		Num     uint16    // number of chunks (so far)
		Flags   uint16    // bit flags { completed, ...}
		Chunks  []Uchunk
		mdmap   map[string]string // remote object metadata (gets sorted when packing)

		// runtime state
		Lom    *LOM
		Size   int64
		suffix string
		mu     sync.Mutex
	}
)

func NewUfest(id string, lom *LOM) *Ufest {
	startTime := time.Now()
	if id == "" {
		id = cos.GenTAID(startTime)
	}
	return &Ufest{
		ID:      id,
		Created: startTime,
		Chunks:  make([]Uchunk, 0, iniChunksCap),
		Lom:     lom,
		suffix:  chunkNameSepa + id + chunkNameSepa,
	}
}

func (u *Ufest) Completed() bool { return u.Flags&completed == completed }

// NOTE:
// - GetMeta() returns a reference that callers must not mutate
// - alternatively, clone it and call SetMeta
// - on-disk order is deterministic (sorted at pack time)
func (u *Ufest) SetMeta(md map[string]string) error {
	if l := len(md); l > maxMetaKeys {
		return fmt.Errorf("%s: number of metadata entries %d exceeds %d limit", utag, l, maxMetaKeys)
	}
	for k, v := range md {
		if len(k) > maxMetaEntryLen || len(v) > maxMetaEntryLen {
			return fmt.Errorf("%s: metadata entry too large (%d, %d, %d)", utag, len(k), len(v), maxMetaEntryLen)
		}
	}
	u.mdmap = md
	return nil
}
func (u *Ufest) GetMeta() map[string]string { return u.mdmap }

func (u *Ufest) Lock()   { u.mu.Lock() }
func (u *Ufest) Unlock() { u.mu.Unlock() }

func (u *Ufest) Add(c *Uchunk, size, num int64) error {
	if size > maxChunkSize {
		return fmt.Errorf("%s [add] chunk size %d exceeds %d limit", utag, size, maxChunkSize)
	}
	c.Siz = size
	if num > math.MaxUint16 || len(u.Chunks) >= math.MaxUint16 {
		return fmt.Errorf("%s [add] chunk number (%d, %d) exceeds %d limit", utag, num, len(u.Chunks), math.MaxUint16)
	}
	c.Num = uint16(num)

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
			return fmt.Errorf("%s [add] failed to replace chunk [%d, %s]: %v", utag, c.Num, dup.Path, err)
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
		return "", errors.New(utag + ": nil lom")
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

	u.Flags = completed

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
	b := sgl.Bytes()
	if err := lom.setXchunk(b); err != nil {
		u.Flags &^= completed
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
	var (
		b64 [cos.SizeofI64]byte
		b16 [cos.SizeofI16]byte
	)

	// meta-version
	w.Write([]byte{umetaver})

	// ID
	_packStr(w, u.ID)

	// creation time
	binary.BigEndian.PutUint64(b64[:], uint64(u.Created.UnixNano()))
	w.Write(b64[:])

	// number of chunks
	binary.BigEndian.PutUint16(b16[:], u.Num)
	w.Write(b16[:])
	// flags
	binary.BigEndian.PutUint16(b16[:], u.Flags)
	w.Write(b16[:])

	// metadata
	l := len(u.mdmap)
	binary.BigEndian.PutUint16(b16[:], uint16(l))
	w.Write(b16[:])
	if l > 0 {
		mdslice := make([]mdpair, 0, l)
		for k, v := range u.mdmap {
			mdslice = append(mdslice, mdpair{k, v})
		}
		sort.Slice(mdslice, func(i, j int) bool { return mdslice[i].k < mdslice[j].k }) // deterministic
		for _, kv := range mdslice {
			_packStr(w, kv.k)
			_packStr(w, kv.v)
		}
		clear(mdslice)
	}

	// chunks
	for _, c := range u.Chunks {
		// chunk size
		binary.BigEndian.PutUint64(b64[:], uint64(c.Siz))
		w.Write(b64[:])

		// path
		_packStr(w, c.Path)

		// checksum
		_packCksum(w, c.Cksum)

		// chunk number and flags
		binary.BigEndian.PutUint16(b16[:], c.Num)
		w.Write(b16[:])
		binary.BigEndian.PutUint16(b16[:], c.Flags)
		w.Write(b16[:])

		// MD5 (legacy)
		_packStr(w, c.MD5)
	}
}

func _packStr(w io.Writer, s string) {
	var b16 [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(b16[:], uint16(len(s)))
	w.Write(b16[:])
	w.Write(cos.UnsafeB(s))
}

func _packCksum(w io.Writer, cksum *cos.Cksum) {
	if cos.NoneC(cksum) {
		_packStr(w, "")
		return
	}
	_packStr(w, cksum.Ty())
	_packStr(w, cksum.Val())
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
	u.Created = time.Unix(0, timeNano)
	offset += cos.SizeofI64

	// number of chunks
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	u.Num = binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16
	// flags
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	u.Flags = binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16

	// metadata map
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	metaCount := binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16

	if metaCount > 0 {
		u.mdmap = make(map[string]string, metaCount)
		for range metaCount {
			var k, v string
			if k, offset, err = _unpackStr(data, offset); err != nil {
				return err
			}
			if v, offset, err = _unpackStr(data, offset); err != nil {
				return err
			}
			u.mdmap[k] = v
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
		if c.Cksum, offset, err = _unpackCksum(data, offset); err != nil {
			return err
		}

		// chunk number and flags
		if len(data) < offset+cos.SizeofI16 {
			return errors.New(tooShort)
		}
		c.Num = binary.BigEndian.Uint16(data[offset:])
		offset += cos.SizeofI16
		if len(data) < offset+cos.SizeofI16 {
			return errors.New(tooShort)
		}
		c.Flags = binary.BigEndian.Uint16(data[offset:])
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

func _unpackCksum(data []byte, offset int) (cksum *cos.Cksum, off int, err error) {
	var (
		ty, val string
	)
	ty, off, err = _unpackStr(data, offset)
	if err != nil || ty == "" {
		return nil, off, err
	}
	offset = off
	val, off, err = _unpackStr(data, offset)
	if err == nil {
		cksum = cos.NewCksum(ty, val)
	}
	return
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
