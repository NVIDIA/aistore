// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

// TODO:
// - remove or isolate `testing` bits -- FIXME
// - readFile vs caller's intent      -- FIXME
// - t.completeMpt() locks/unlocks two times - consider CoW
// - remove all debug.AssertFunc
// - space cleanup; orphan chunks
// - err-busy
// - warn if bucket.mirror.enabled

const (
	umetaver = 1
)

const (
	iniChunksCap = 32
)

const (
	sizeLoad  = memsys.DefaultBufSize  // SGL buffer sizing
	sizeStore = memsys.MaxPageSlabSize // ditto // (sgl.Bytes())
)

const (
	// 0001..9999 are zero-padded for lexicographic order
	// >=10000 uses raw num - update when needed
	fmtChunkNum = "%04d"
)

// single flag so far
const (
	flCompleted uint16 = 1 << 0 // Ufest.flags
)

const (
	utag     = "chunk-manifest"
	itag     = "invalid " + utag
	tooShort = "failed to unpack: too short"
)

type (
	Uchunk struct {
		size  int64      // this chunk size
		Path  string     // (may become v2 _remote location_)
		Cksum *cos.Cksum // nil means none; otherwise non-empty
		num   uint16     // chunk/part number
		flags uint16     // bit flags (future use)
		// S3/legacy (either/or)
		MD5  []byte
		ETag string
	}
	Ufest struct {
		ID      string    // upload/manifest ID
		Created time.Time // creation time
		count   uint16    // number of chunks (so far)
		flags   uint16    // bit flags { completed, ...}
		Chunks  []Uchunk

		// runtime state
		Lom       *LOM
		size      int64
		completed atomic.Bool
		mu        sync.Mutex
	}
)

var (
	errNoChunks   = errors.New("no chunks")
	errNoChunkOne = errors.New("no chunk #1")
)

////////////
// Uchunk //
////////////

func (c *Uchunk) Size() int64 { return c.size }
func (c *Uchunk) Num() uint16 { return c.num }

///////////
// Ufest //
///////////

func NewUfest(id string, lom *LOM, mustExist bool) *Ufest {
	now := time.Now()
	if id == "" && !mustExist {
		id = cos.GenTAID(now)
	}
	return &Ufest{
		ID:      id,
		Created: now,
		Chunks:  make([]Uchunk, 0, iniChunksCap),
		Lom:     lom,
	}
}

// immutable once completed
func (u *Ufest) Completed() bool { return u.completed.Load() }

func (u *Ufest) Size() int64 { return u.size }
func (u *Ufest) Count() int  { return int(u.count) }

func (u *Ufest) Lock()   { u.mu.Lock() }
func (u *Ufest) Unlock() { u.mu.Unlock() }

func (u *Ufest) Add(c *Uchunk, size, num int64) error {
	if num <= 0 {
		return fmt.Errorf("%s: invalid chunk number: %d (must be > 0)", utag, num)
	}
	c.size = size
	if num > math.MaxUint16 || len(u.Chunks) >= math.MaxUint16 {
		return fmt.Errorf("%s [add] chunk number (%d, %d) exceeds %d limit", utag, num, len(u.Chunks), math.MaxUint16)
	}
	c.num = uint16(num)

	u.mu.Lock()
	defer u.mu.Unlock()

	l := len(u.Chunks)
	// append
	if l == 0 || u.Chunks[l-1].num < c.num {
		u.Chunks = append(u.Chunks, *c)
		u.size += c.size
		u.count = uint16(len(u.Chunks))
		return nil
	}

	idx := sort.Search(l, func(i int) bool { return u.Chunks[i].num >= c.num })

	// replace ("last wins")
	dup := &u.Chunks[idx]
	if idx < l && dup.num == c.num {
		if err := cos.RemoveFile(dup.Path); err != nil {
			return fmt.Errorf("%s [add] failed to replace chunk [%d, %s]: %v", utag, c.num, dup.Path, err)
		}
		u.size += c.size - dup.size
		*dup = *c
		return nil
	}

	// insert
	u.Chunks = append(u.Chunks, Uchunk{})
	copy(u.Chunks[idx+1:], u.Chunks[idx:])
	u.Chunks[idx] = *c
	u.size += c.size
	u.count = uint16(len(u.Chunks))
	return nil
}

func (u *Ufest) GetChunk(num uint16, locked bool) *Uchunk {
	if !locked {
		u.mu.Lock()
		defer u.mu.Unlock()
	}
	if int(num) <= len(u.Chunks) && u.Chunks[num-1].num == num {
		return &u.Chunks[num-1]
	}
	for i := range u.Chunks {
		if u.Chunks[i].num == num {
			return &u.Chunks[i]
		}
	}
	return nil
}

func (u *Ufest) Abort(lom *LOM) error {
	u.mu.Lock()
	for i := range u.Chunks {
		c := &u.Chunks[i]
		if err := cos.RemoveFile(c.Path); err != nil {
			if cmn.Rom.FastV(4, cos.SmoduleCore) {
				nlog.Warningln("abort", utag, "- failed to remove chunk(s) [", u.ID, lom.Cname(), c.Path, err, "]")
			}
		}
	}
	if u.ID != "" {
		partial, _ := u.fqns(lom)
		if err := cos.RemoveFile(partial); err != nil {
			if cmn.Rom.FastV(4, cos.SmoduleCore) {
				nlog.Warningln("abort", utag, "- failed to remove partial manifest [", u.ID, lom.Cname(), err, "]")
			}
		}
	}
	u.mu.Unlock()
	return nil
}

// Generate chunk file path - the `num` here is Uchunk.num
// - chunk #1 stays on the object's mountpath
// - other chunks get HRW distributed
func (u *Ufest) ChunkFQN(num int) (string, error) {
	lom := u.Lom
	if lom == nil {
		return "", errors.New(utag + ": nil lom")
	}
	if num <= 0 {
		return "", fmt.Errorf("%s: invalid chunk number (%d)", u._itag(lom.Cname()), num)
	}
	var (
		contentResolver = fs.CSM.Resolver(fs.ChunkCT)
		chname          = contentResolver.MakeFQN(lom.ObjName, u.ID, fmt.Sprintf(fmtChunkNum, num))
	)
	if num > 9999 {
		chname = contentResolver.MakeFQN(lom.ObjName, u.ID, strconv.Itoa(num))
	}
	if num == 1 {
		return lom.Mountpath().MakePathFQN(lom.Bucket(), fs.ChunkCT, chname), nil
	}
	mi, _ /*digest*/, err := fs.Hrw(cos.UnsafeB(chname))
	return mi.MakePathFQN(lom.Bucket(), fs.ChunkCT, chname), err
}

// must be called under lom rlock
// do not validate manifest ID since
// callers may use temporary/generated IDs when loading existing manifests
// (during GET)
func (u *Ufest) Load(lom *LOM) error {
	debug.Assert(lom.IsLocked() > apc.LockNone, "expecting locked: ", lom.Cname())
	if u.Lom == nil {
		u.Lom = lom
	}
	debug.Assert(u.Lom == lom && lom != nil)

	csgl := g.pmm.NewSGL(sizeLoad)
	defer csgl.Free()

	completed, err := u.readFile(lom, csgl)
	if err != nil {
		return u._errLoad(lom.Cname(), err)
	}
	if err := u._load(lom, csgl); err != nil {
		return err
	}
	if completed {
		if size := lom.Lsize(true); size != u.size {
			return fmt.Errorf("%s load size mismatch: manifest %d vs %d lom", u._itag(lom.Cname()), u.size, size)
		}
		debug.Assert(u.flags&flCompleted == flCompleted)
		u.completed.Store(u.flags&flCompleted == flCompleted)
	}
	return nil
}

func (u *Ufest) _load(lom *LOM, csgl *memsys.SGL) error {
	data := csgl.Bytes()

	// validate and strip/remove the trailing checksum
	checksumOffset := len(data) - cos.SizeXXHash64
	expectedChecksum := binary.BigEndian.Uint64(data[checksumOffset:])
	compressedData := data[:checksumOffset]
	actualChecksum := onexxh.Checksum64S(compressedData, cos.MLCG32)
	if expectedChecksum != actualChecksum {
		return cos.NewErrMetaCksum(expectedChecksum, actualChecksum, utag)
	}

	givenID := u.ID

	// decompress into a 2nd buffer
	dbuf, dslab := g.pmm.AllocSize(sizeLoad)
	err := u._loadCompressed(lom, compressedData, dbuf)
	dslab.Free(dbuf)

	debug.Assert(u.validNums() == nil)

	if givenID != "" && givenID != u.ID {
		return fmt.Errorf("%s ID mismatch: given %q vs %q stored", u._itag(lom.Cname()), givenID, u.ID)
	}

	u.Lom = lom
	return err
}

func (u *Ufest) _loadCompressed(lom *LOM, compressedData, buf []byte) error {
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
		return u._errLoad(lom.Cname(), err)
	}
	return nil
}

func (u *Ufest) _errLoad(cname string, err error) error {
	return fmt.Errorf("failed to load %s: %w", u._itag(cname), err)
}

func (u *Ufest) _itag(cnames ...string) string {
	var cname string
	if len(cnames) > 0 {
		cname = cnames[0]
	} else if u.Lom != nil {
		cname = u.Lom.Cname()
	}
	return itag + "[" + cname + "@" + u.ID + "]"
}

func (u *Ufest) StoreCompleted(lom *LOM, testing ...bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// validate
	if err := u._errCompleted(lom); err != nil {
		return err
	}
	if u.count == 0 || int(u.count) != len(u.Chunks) {
		return fmt.Errorf("%s: num %d vs %d", u._itag(lom.Cname()), u.count, len(u.Chunks))
	}

	lsize := lom.Lsize(true)
	if u.size != 0 && u.size != lsize {
		return fmt.Errorf("%s store size: %d vs %d", u._itag(lom.Cname()), u.size, lsize)
	}

	var total int64
	for i := range u.count {
		total += u.Chunks[i].size
	}
	if total != lsize {
		return fmt.Errorf("%s: total size mismatch (%d vs %d)", u._itag(lom.Cname()), total, lsize)
	}

	if err := u.validNums(); err != nil {
		return fmt.Errorf("%s: failed to store, err: %v", u._itag(lom.Cname()), err)
	}

	// fixup chunk #1
	c, e := u.firstChunk()
	if e != nil {
		return e
	}
	orig := c.Path
	if len(testing) == 0 {
		if err := lom.RenameFinalize(c.Path); err != nil {
			return err
		}
	}
	c.Path = lom.FQN

	// store
	u.flags |= flCompleted
	u.completed.Store(true)

	sgl := g.pmm.NewSGL(u._packSize())

	debug.Assert(c.Path == lom.FQN)
	partial, err := u._store(lom, sgl, true)
	sgl.Free()

	if err == nil {
		err2 := cos.RemoveFile(partial)
		debug.AssertNoErr(err2)
		return nil
	}

	// undo
	u.flags &^= flCompleted // undo
	u.completed.Store(false)
	if len(testing) == 0 {
		if nerr := cos.Rename(lom.FQN, orig); nerr != nil {
			nlog.Errorf("failed to store %s: w/ nested error [%v, %v]", u._itag(lom.Cname()), err, nerr)
			T.FSHC(err, lom.Mountpath(), lom.FQN)
		} else {
			c.Path = orig
		}
	} else {
		c.Path = orig
	}
	return err
}

func (u *Ufest) _errCompleted(lom *LOM) error {
	if u.Completed() {
		return errors.New(u._itag(lom.Cname()) + ": already completed")
	}
	return nil
}

func (u *Ufest) validNums() error {
	if u.count == 0 {
		return errNoChunks
	}
	for i := range u.Chunks {
		c := &u.Chunks[i]
		if c.num <= 0 || c.num > u.count {
			return fmt.Errorf("chunk %d has invalid part number [%d, %d]", i, c.num, u.count)
		}
		for j := range i {
			if u.Chunks[j].num == c.num {
				return fmt.Errorf("duplicate chunk number: [%d, %d, %d]", c.num, i, j)
			}
		}
	}
	return nil
}

func (u *Ufest) StorePartial(lom *LOM) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if err := u._errCompleted(lom); err != nil {
		return err
	}
	if u.ID == "" || u.count == 0 {
		return fmt.Errorf("%s: partial manifest must have a valid ID and at least one chunk (have %q, %d)",
			u._itag(lom.Cname()), u.ID, u.count)
	}

	u.flags &^= flCompleted // always incomplete here
	sgl := g.pmm.NewSGL(u._packSize())
	_, err := u._store(lom, sgl, false)
	sgl.Free()
	return err
}

func (u *Ufest) _packSize() int64 {
	estimated := int64(len(u.Chunks)) * 128 // ~100 bytes per chunk (paths are typically 50-100 chars)
	return max(sizeStore, estimated)
}

func (u *Ufest) _store(lom *LOM, sgl *memsys.SGL, completed bool) (string, error) {
	debug.Assert(u.ID != "")

	// compress
	zw := lz4.NewWriter(sgl)
	u.pack(zw)
	zw.Close()

	// compute and write trailing checksum
	data := sgl.Bytes()
	h := onexxh.Checksum64S(data, cos.MLCG32)
	var checksumBuf [cos.SizeXXHash64]byte
	binary.BigEndian.PutUint64(checksumBuf[:], h)
	sgl.Write(checksumBuf[:])

	// write
	return u.writeFile(lom, sgl, completed)
}

// note that Add() keeps chunks sorted by their respective numbers,
// so Chunks[0] is the lowest present at any time
func (u *Ufest) firstChunk() (*Uchunk, error) {
	debug.AssertFunc(func() bool {
		for i := 1; i < len(u.Chunks); i++ {
			if u.Chunks[i-1].num > u.Chunks[i].num {
				return false
			}
		}
		return true
	})

	if err := u._check(); err != nil {
		return nil, err
	}
	return &u.Chunks[0], nil
}

func (u *Ufest) _check() error {
	if len(u.Chunks) == 0 {
		return errNoChunks
	}
	if u.Chunks[0].num != 1 {
		return errNoChunkOne
	}
	return nil
}

//
// readFile | writeFile
// (completed | partial)
//

func (u *Ufest) fqns(lom *LOM) (partial, fqn string) {
	if u.ID != "" {
		partial = fs.CSM.Gen(lom, fs.ChunkMetaCT, u.ID)
	}
	fqn = fs.CSM.Gen(lom, fs.ChunkMetaCT)
	return
}

// TODO -- FIXME: caller should be able to specify the intent: load completed OR load partial
// TODO: currently, always under LOM read or write lock, even when partial
func (u *Ufest) readFile(lom *LOM, sgl *memsys.SGL) (completed bool, err error) {
	var (
		fh           *os.File
		partial, fqn = u.fqns(lom)
	)
	if partial != "" {
		fh, err = os.Open(partial)
	}
	if partial == "" || err != nil {
		fh, err = os.Open(fqn)
		if err != nil {
			return false, err
		}
		completed = true
	}
	defer cos.Close(fh)

	var n int64
	n, err = io.Copy(sgl, fh)

	if err != nil {
		return false, err
	}
	debug.Assert(n == sgl.Len())
	return completed, nil
}

// locking rules:
// - there's always manifest mutex
// - if the caller is completing this manifest there must be LOM wlock
func (u *Ufest) writeFile(lom *LOM, sgl *memsys.SGL, completed bool) (string, error) {
	debug.Assert(u.Completed() == completed)
	debug.Assert(!completed || lom.IsLocked() == apc.LockWrite, "expecting w-locked when completed: ", lom.Cname())

	partial, fqn := u.fqns(lom)
	if !completed {
		fqn = partial
	}

	wfh, err := cos.CreateFile(fqn)
	if err != nil {
		return "", err
	}
	defer cos.Close(wfh)

	l := sgl.Len()
	written, erw := io.Copy(wfh, sgl)
	if erw != nil {
		return "", erw
	}
	if written != l {
		return "", fmt.Errorf("%s: invalid write size %d != %d", u._itag(lom.Cname()), written, l) // (unlikely)
	}
	return partial, nil
}

//
// checksum and size of the _WHOLE_ ---------------------------------------------------------------
//

func (u *Ufest) ETagS3() (string, error) {
	debug.Assert(u.count > 0 && int(u.count) == len(u.Chunks), "invalid chunks num ", u.count, " vs ", len(u.Chunks))

	h := md5.New()
	for i := range u.count {
		c := &u.Chunks[i]
		switch {
		case len(c.MD5) == md5.Size:
			h.Write(c.MD5)
			if c.ETag != "" {
				if bin, err := cmn.ETagToMD5(c.ETag); err == nil {
					debug.Assert(bytes.Equal(bin, c.MD5),
						"ETag/md5 mismatch: ", c.ETag, " vs ", cmn.MD5ToETag(c.MD5))
				}
			}
		case c.ETag != "":
			bin, err := cmn.ETagToMD5(c.ETag)
			if err == nil && len(bin) == md5.Size {
				h.Write(bin)
				continue
			}
			fallthrough
		default:
			err := fmt.Errorf("%s: invalid ETag for part %d: %q", u._itag(), i+1, c.ETag)
			debug.AssertNoErr(err)
			return "", err
		}
	}
	// S3 compliant multipart ETag has the following format:
	// (note: should there be a special case for u.count == 1?)
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)
	return `"` + s + cmn.AwsMultipartDelim + strconv.Itoa(int(u.count)) + `"`, nil
}

// reread all chunk payloads to compute a checksum of the given type
// TODO: avoid the extra pass by accumulating during AddPart/StorePartial or by caching a tree-hash
// see also: s3/mpt for ListParts
func (u *Ufest) ComputeWholeChecksum(cksumH *cos.CksumHash) error {
	debug.Assert(u.count > 0 && int(u.count) == len(u.Chunks), "invalid chunks num ", u.count, " vs ", len(u.Chunks))

	c, err := u.firstChunk()
	if err != nil {
		return err
	}

	var written int64
	buf, slab := g.pmm.AllocSize(c.size)
	defer slab.Free(buf)

	for i := range u.count {
		c := &u.Chunks[i]
		fh, err := os.Open(c.Path)
		if err != nil {
			return err
		}
		nn, e := io.CopyBuffer(cksumH.H, fh, buf)
		cos.Close(fh)
		if e != nil {
			return e
		}
		if nn != c.size {
			return fmt.Errorf("%s: invalid size for part %d: got %d, want %d", u._itag(), c.num, nn, c.size)
		}
		written += nn
	}

	debug.Assert(written == u.size, "invalid chunks total size ", written, " vs ", u.size)
	cksumH.Finalize()
	return nil
}

//
// pack -- unpack -- read/write --------------------------------
//

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
	binary.BigEndian.PutUint16(b16[:], u.count)
	w.Write(b16[:])
	// flags
	binary.BigEndian.PutUint16(b16[:], u.flags)
	w.Write(b16[:])

	// chunks
	for _, c := range u.Chunks {
		// chunk size
		binary.BigEndian.PutUint64(b64[:], uint64(c.size))
		w.Write(b64[:])

		// path
		_packStr(w, c.Path)

		// checksum
		_packCksum(w, c.Cksum)

		// chunk number and flags
		binary.BigEndian.PutUint16(b16[:], c.num)
		w.Write(b16[:])
		binary.BigEndian.PutUint16(b16[:], c.flags)
		w.Write(b16[:])

		// S3 legacy
		_packBytes(w, c.MD5)
		_packStr(w, c.ETag)
	}
}

func _packStr(w io.Writer, s string) {
	_packBytes(w, cos.UnsafeB(s))
}

func _packBytes(w io.Writer, b []byte) {
	var b16 [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(b16[:], uint16(len(b)))
	w.Write(b16[:])
	w.Write(b)
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
	u.count = binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16
	// flags
	if len(data) < offset+cos.SizeofI16 {
		return errors.New(tooShort)
	}
	u.flags = binary.BigEndian.Uint16(data[offset:])
	offset += cos.SizeofI16

	// Read chunks
	u.Chunks = make([]Uchunk, u.count)
	u.size = 0
	for i := range u.count {
		c := &u.Chunks[i]

		// chunk size
		if len(data) < offset+cos.SizeofI64 {
			return errors.New(tooShort)
		}
		c.size = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += cos.SizeofI64
		u.size += c.size

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
		c.num = binary.BigEndian.Uint16(data[offset:])
		offset += cos.SizeofI16
		if len(data) < offset+cos.SizeofI16 {
			return errors.New(tooShort)
		}
		c.flags = binary.BigEndian.Uint16(data[offset:])
		offset += cos.SizeofI16

		// S3 legacy
		if c.MD5, offset, err = _unpackBytes(data, offset); err != nil {
			return err
		}
		if l := len(c.MD5); l != 0 && l != md5.Size {
			return fmt.Errorf("invalid MD5 size %d", l)
		}
		if c.ETag, offset, err = _unpackStr(data, offset); err != nil {
			return err
		}
	}
	return nil
}

func _unpackStr(data []byte, offset int) (s string, off int, err error) {
	var b []byte
	b, off, err = _unpackBytes(data, offset)
	if err == nil {
		s = string(b)
	}
	return
}

func _unpackBytes(data []byte, offset int) ([]byte, int, error) {
	if len(data) < offset+cos.SizeofI16 {
		return nil, offset, errors.New(tooShort)
	}

	l := int(binary.BigEndian.Uint16(data[offset:]))
	offset += cos.SizeofI16
	if len(data) < offset+l {
		return nil, offset, errors.New(tooShort)
	}
	return data[offset : offset+l], offset + l, nil
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
// LOM: chunk persistence ------------------------------------------------------
//

func (lom *LOM) CompleteUfest(u *Ufest) error {
	lom.Lock(true)
	defer lom.Unlock(true)

	debug.AssertFunc(func() bool {
		var total int64
		for i := range u.count {
			total += u.Chunks[i].size
		}
		return total == u.size
	})

	lom.SetSize(u.size) // StoreCompleted still performs non-debug validation
	if err := u.StoreCompleted(lom); err != nil {
		lom.abortUfest(u, err)
		return err
	}
	lom.SetAtimeUnix(u.Created.UnixNano())

	lom.setlmfl(lmflChunk)
	debug.Assert(lom.md.lid.haslmfl(lmflChunk))

	if err := lom.PersistMain(); err != nil {
		lom.md.lid.clrlmfl(lmflChunk)
		lom.abortUfest(u, err)
		return err
	}

	return nil
}

// rollback w/ full cleanup
func (lom *LOM) abortUfest(u *Ufest, err error) {
	if nerr := u.Abort(lom); nerr != nil {
		nlog.Errorf("failed to complete %s: w/ nested error [%v, %v]", u._itag(lom.Cname()), err, nerr)
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	}
}

/////////////////
// UfestReader //
/////////////////

// - is a LomReader
// - not thread-safe on purpose and by design
// - usage pattern: (construct -- read -- close) in a single goroutine

type (
	UfestReader struct {
		// parent
		u *Ufest
		// chunk
		cfh  *os.File
		coff int64
		cidx int
		// global
		goff int64
	}
)

var (
	_ cos.LomReader = (*UfestReader)(nil)
)

func (lom *LOM) NewUfestReader() (cos.LomReader, error) {
	debug.Assert(lom.IsLocked() > apc.LockNone, "expecting locked: ", lom.Cname())

	u := NewUfest("", lom, true)
	if err := u.Load(lom); err != nil {
		return nil, err
	}
	if !u.Completed() {
		return nil, fmt.Errorf("%s: cannot read partial manifest", u._itag(lom.Cname()))
	}
	return u.NewReader()
}

func (u *Ufest) NewReader() (*UfestReader, error) {
	if !u.Completed() {
		return nil, fmt.Errorf("%s: is incomplete - cannot be read", u._itag())
	}
	return &UfestReader{u: u}, nil
}

func (r *UfestReader) Read(p []byte) (n int, err error) {
	u := r.u
	for len(p) > 0 {
		// done
		if r.cidx >= int(u.count) {
			debug.Assert(r.goff == u.size, "offset ", r.goff, " vs full size ", u.size)
			return n, io.EOF
		}
		c := &u.Chunks[r.cidx]

		// open on demand
		if r.cfh == nil {
			debug.Assert(r.coff == 0)
			r.cfh, err = os.Open(c.Path)
			if err != nil {
				return n, err
			}
		}

		// read
		var (
			m   int
			rem = min(c.size-r.coff, int64(len(p)))
		)
		if rem > 0 {
			m, err = r.cfh.Read(p[:rem])
			n += m
		}
		p = p[m:]
		r.coff += int64(m)
		r.goff += int64(m)
		debug.Assert(r.goff <= u.size)
		debug.Assert(r.coff <= c.size)

		switch err {
		case io.EOF:
			if r.coff < c.size {
				return n, io.ErrUnexpectedEOF // truncated
			}
			err = nil
		case nil:
			if rem > int64(m) {
				// unlikely (but legal) short read; keep filling from the current chunk
				continue
			}
		default:
			return n, err
		}

		// next chunk
		if r.coff >= c.size {
			cos.Close(r.cfh)
			r.cfh = nil
			r.coff = 0
			r.cidx++

			if len(p) == 0 {
				return n, nil
			}
		}
	}

	return n, nil
}

func (r *UfestReader) Close() error {
	if r.cfh != nil {
		cos.Close(r.cfh)
		r.cfh = nil
	}
	return nil
}

// consistent with io.ReaderAt semantics:
// - do not use or modify reader's offset (r.goff) or any other state
// - open the needed chunk(s) transiently and close them immediately
// - return io.EOF when less than len(p)
func (r *UfestReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New(utag + ": negative offset")
	}
	u := r.u
	if off >= u.size {
		return 0, io.EOF
	}

	// skip to position
	var (
		idx      int
		chunkoff = off
	)
	for ; idx < len(u.Chunks) && chunkoff >= u.Chunks[idx].size; idx++ {
		chunkoff -= u.Chunks[idx].size
	}
	if idx >= len(u.Chunks) {
		return 0, io.EOF
	}

	total := len(p)
	if total == 0 {
		return 0, nil
	}
	// read
	for n < total && idx < len(u.Chunks) {
		c := &u.Chunks[idx]
		debug.Assert(c.size-chunkoff > 0, c.size, " vs ", chunkoff)
		toRead := min(int64(total-n), c.size-chunkoff)
		fh, err := os.Open(c.Path)
		if err != nil {
			return n, err
		}

		var m int
		m, err = fh.ReadAt(p[n:n+int(toRead)], chunkoff)
		cos.Close(fh)

		n += m
		if err != nil {
			return n, err
		}

		// 2nd, etc. chunks
		idx++
		chunkoff = 0
	}

	if n < total {
		err = io.EOF
	}
	return n, err
}
