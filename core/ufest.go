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

// The `Ufest` structure (below) is the persistent manifest for a chunked object.
// It records an ordered list of chunks (`Uchunk`) with their respective sizes,
// paths, checksums, and optional S3-compat fields (MD5/ETag).
//
// Like all persistent metadata in AIStore, each manifest is meta-versioned
// and checksummed (with the checksum written last). On disk, manifests are
// also lz4-compressed (in memory they, of course, remain uncompressed).
//
// Lifecycle:
//   * Partial manifests are created during upload and checkpointed every N
//     chunks (configurable). Multiple parallel uploads are supported, each with
//     its own manifest ID.
//   * Once all chunks are present, `CompleteUfest()` atomically finalizes the
//     manifest, marks the LOM as chunked, and persists the association.
//   * At any time there is only one "completed" manifest per object. Old
//     manifests and orphaned chunks are cleaned up automatically (or by
//     CLI `space-cleanup`).
//
// Chunks:
//   * Strong numbering (numbered chunks can be added in any order)
//   * No limit on number of chunks (maximum size is 5GiB).
//   * Whole-object checksum and S3 multipart ETag derivable from chunks.
//   * Chunked and monolithic formats are transparently interchangeable for
//     reads, including range reads and archive reads.
//
// Completion:
//   * Completion rules follow S3 semantics with one clarification: we require
//     the complete `[1..count]` set to finalize.
//   * Chunks (a.k.a., parts) may arrive **unordered**, duplicates are tolerated,
//     and the most recent copy of a given `partNumber` (chunk mumber) wins.
//   * Partial completion is rejected.
//
// Concurrency / locking:
//   * Each manifest instance has its own binary mutex (not RW), used to facilitate
//     concurrent access vs internal state changes.
//   * LOM locks are always outermost; manifest locks nest inside. For example,
//     `CompleteUfest` =>  `storeCompleted` holds object wlock, then manifest mutex.
//   * Readers (`UfestReader` below) and writers coordinate via these locks.
//
// See methods: Add(), StorePartial(), LoadCompleted(), NewUfestReader().
//
// TODO: add stats counters; (eventually) remove debug.AssertFunc, (try-lock + err-busy)

const (
	umetaver = 1
)

const (
	iniChunksCap = 32
)

const (
	sizeLoad  = memsys.DefaultBufSize  // SGL buffer sizing
	sizeStore = memsys.MaxPageSlabSize // ditto // (sgl.Bytes())

	packedChunkSize = 256 // estimated max
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

	tagCompleted = "completed"
	tagPartial   = "partial"
)

type (
	Uchunk struct {
		size  int64      // this chunk size
		path  string     // (may become v2 _remote location_)
		cksum *cos.Cksum // nil means none; otherwise non-empty
		num   uint16     // chunk/part number
		flags uint16     // bit flags (compression; future use)
		// S3/legacy (either/or)
		MD5  []byte
		ETag string
	}
	Ufest struct {
		id      string    // upload/manifest ID
		created time.Time // creation time
		count   uint16    // number of chunks (so far)
		flags   uint16    // bit flags { completed, ...}
		chunks  []Uchunk

		// runtime state
		lom       *LOM
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

// readonly
func (c *Uchunk) Size() int64  { return c.size }
func (c *Uchunk) Num() uint16  { return c.num }
func (c *Uchunk) Path() string { return c.path }

// validate and set
func (c *Uchunk) SetCksum(cksum *cos.Cksum) {
	if !cos.NoneC(cksum) {
		debug.AssertNoErr(cksum.Validate())
	}
	c.cksum = cksum
}

func (c *Uchunk) SetETag(etag string) { c.ETag = etag }

///////////
// Ufest //
///////////

func NewUfest(id string, lom *LOM, mustExist bool) (*Ufest, error) {
	if id != "" {
		if err := cos.ValidateManifestID(id); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	if id == "" && !mustExist {
		id = cos.GenTAID(now)
	}
	u := &Ufest{
		id:      id,
		created: now,
		chunks:  make([]Uchunk, 0, iniChunksCap),
		lom:     lom,
	}
	return u, nil
}

// immutable once completed
func (u *Ufest) Completed() bool { return u.completed.Load() }

func (u *Ufest) Lom() *LOM          { return u.lom }
func (u *Ufest) Size() int64        { return u.size }
func (u *Ufest) Count() int         { return int(u.count) }
func (u *Ufest) Created() time.Time { return u.created }
func (u *Ufest) ID() string         { return u.id }

func (u *Ufest) Lock()   { u.mu.Lock() }
func (u *Ufest) Unlock() { u.mu.Unlock() }

func _nolom(lom *LOM) error {
	if lom == nil || lom.mi == nil {
		return errors.New(utag + ": lom is nil or uninitialized")
	}
	return nil
}

// construct a new chunk and its pathname
// - `num` is not an index (must start from 1)
// - chunk #1 stays on the object's mountpath
// - other chunks get HRW distributed
func (u *Ufest) NewChunk(num int, lom *LOM) (*Uchunk, error) {
	if err := _nolom(u.lom); err != nil {
		return nil, err
	}
	if err := _nolom(lom); err != nil {
		return nil, err
	}
	// TODO: strict and simple check (ie., not supporting mountpath changes during upload)
	if lom.FQN != u.lom.FQN {
		return nil, fmt.Errorf("%s: object mismatch: %s vs (%s, %s)", u._utag(u.lom.Cname()), u.lom.mi, lom.Cname(), lom.mi)
	}
	if num <= 0 || num > math.MaxUint16 {
		return nil, fmt.Errorf("%s: invalid chunk number (%d)", u._utag(lom.Cname()), num)
	}

	var snum string
	if num > 9999 {
		snum = strconv.Itoa(num)
	} else {
		snum = fmt.Sprintf(fmtChunkNum, num)
	}

	// note first chunk's location
	if num == 1 {
		return &Uchunk{
			path: lom.GenFQN(fs.ChunkCT, u.id, snum),
			num:  uint16(num),
		}, nil
	}

	// all the rest chunks
	hrwkey := u.id + snum
	mi, _ /*digest*/, err := fs.Hrw(cos.UnsafeB(hrwkey))
	if err != nil {
		return nil, err
	}

	ct := newChunkCT(lom, mi)
	return &Uchunk{
		path: ct.GenFQN("", u.id, snum),
		num:  uint16(num),
	}, nil
}

func (u *Ufest) Add(c *Uchunk, size, num int64) error {
	// validate
	if err := _nolom(u.lom); err != nil {
		return err
	}
	lom := u.lom
	if c == nil || c.num == 0 || c.path == "" {
		return fmt.Errorf("%s: nil chunk", u._utag(lom.Cname()))
	}
	if num != int64(c.num) {
		return fmt.Errorf("%s: invalid chunk number %d (expecting %d)", u._utag(lom.Cname()), num, c.num)
	}

	c.size = size
	c.num = uint16(num)

	u.mu.Lock()
	defer u.mu.Unlock()

	// add
	err := u._add(c)
	if err != nil {
		return err
	}

	// store
	if n := lom.Bprops().Chunks.CheckpointEvery; n > 0 {
		if u.count%uint16(n) == 0 {
			err = u.StorePartial(lom, true)
		}
	}
	return err
}

func (u *Ufest) _add(c *Uchunk) error {
	l := len(u.chunks)
	// append
	if l == 0 || u.chunks[l-1].num < c.num {
		u.chunks = append(u.chunks, *c)
		u.size += c.size
		u.count = uint16(len(u.chunks))
		return nil
	}

	idx := sort.Search(l, func(i int) bool { return u.chunks[i].num >= c.num })

	// replace ("last wins")
	dup := &u.chunks[idx]
	if idx < l && dup.num == c.num {
		if err := cos.RemoveFile(dup.path); err != nil {
			return fmt.Errorf("%s: failed to replace chunk [%d, %s]: %v", utag, c.num, dup.path, err)
		}
		u.size += c.size - dup.size
		*dup = *c
		return nil
	}

	// insert
	u.chunks = append(u.chunks, Uchunk{})
	copy(u.chunks[idx+1:], u.chunks[idx:])
	u.chunks[idx] = *c
	u.size += c.size
	u.count = uint16(len(u.chunks))

	return nil
}

func (u *Ufest) GetChunk(num int, locked bool) *Uchunk {
	if !locked {
		u.mu.Lock()
		defer u.mu.Unlock()
	}
	debug.Assert(num > 0, num)
	if num <= len(u.chunks) && u.chunks[num-1].num == uint16(num) {
		return &u.chunks[num-1]
	}
	for i := range u.chunks {
		if u.chunks[i].num == uint16(num) {
			return &u.chunks[i]
		}
	}
	return nil
}

func (u *Ufest) removeChunks(lom *LOM, exceptFirst bool) {
	var (
		ecnt int
		skip uint16
	)
	if exceptFirst {
		c, err := u.firstChunk()
		if err != nil {
			nlog.ErrorDepth(1, u._utag(lom.Cname()), err)
			return
		}
		skip = c.num
		debug.Assert(c.num == 1) // validated by u.firstChunk()
	}
	for i := range u.chunks {
		c := &u.chunks[i]
		if exceptFirst && c.num == skip {
			continue
		}
		if err := cos.RemoveFile(c.path); err != nil {
			ecnt++
			if cmn.Rom.V(4, cos.ModCore) || ecnt == 1 {
				nlog.WarningDepth(1, u._utag(lom.Cname())+":",
					"failed to remove chunk [", c.num, c.path, err, ecnt, "]")
			}
		}
	}
	clear(u.chunks)
}

func (u *Ufest) Abort(lom *LOM) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.removeChunks(lom, false /*except first*/)
	if u.id == "" {
		return
	}
	debug.Func(func() {
		err := cos.ValidateManifestID(u.id)
		debug.AssertNoErr(err)
	})
	partial := u._fqns(lom, false)
	if err := cos.RemoveFile(partial); err != nil {
		if cmn.Rom.V(4, cos.ModCore) {
			nlog.Warningln("abort", u._utag(lom.Cname()), "- failed to remove partial manifest [", err, "]")
		}
	}
}

// scenarios: a) remove-obj; b) update/overwrite (when exceptFirst = true)
func (u *Ufest) removeCompleted(exceptFirst bool) error {
	lom := u.lom
	debug.Assert(lom.IsLocked() > apc.LockNone, "expecting locked", lom.Cname())
	u.mu.Lock()
	defer u.mu.Unlock()

	if err := u.load(true); err != nil {
		if cos.IsNotExist(err) {
			return nil
		}
		return err
	}
	u.removeChunks(lom, exceptFirst)
	fqn := u._fqns(lom, true)
	return cos.RemoveFile(fqn)
}

// must be called under lom (r)lock
func (u *Ufest) LoadCompleted(lom *LOM) error {
	debug.Assert(lom.IsLocked() > apc.LockNone, "expecting locked", lom.Cname())
	debug.Assert(u.lom == lom || u.lom == nil)
	u.lom = lom

	if u.id != "" {
		if err := cos.ValidateManifestID(u.id); err != nil {
			return fmt.Errorf("%s %v: [%s]", tagCompleted, err, lom.Cname())
		}
	}

	if err := u.load(true); err != nil {
		return err
	}

	if size := lom.Lsize(true); size != u.size {
		return fmt.Errorf("%s load size mismatch: %s manifest-recorded size %d vs object size %d",
			u._itag(lom.Cname()), tagCompleted, u.size, size)
	}

	if u.flags&flCompleted == 0 { // (unlikely)
		debug.Assert(false)
		return fmt.Errorf("%s %s: not marked 'completed'", tagCompleted, u._utag(lom.Cname()))
	}
	u.completed.Store(true)
	return nil
}

func (u *Ufest) LoadPartial(lom *LOM) error {
	debug.Assert(lom.IsLocked() > apc.LockNone, "expecting locked", lom.Cname())

	if err := cos.ValidateManifestID(u.id); err != nil {
		return fmt.Errorf("%s %s manifest: %v", tagPartial, u._utag(lom.Cname()), err)
	}
	debug.Assert(u.lom == lom)
	u.lom = lom

	err := u.load(false /*completed*/)
	debug.Assert(u.flags&flCompleted == 0)
	return err
}

func (u *Ufest) load(completed bool) error {
	csgl := g.pmm.NewSGL(sizeLoad)
	defer csgl.Free()

	tag := tagPartial
	if completed {
		tag = tagCompleted
	}
	if err := u.fread(csgl, completed); err != nil {
		return u._errLoad(tag, u.lom.Cname(), err)
	}
	return u._load(csgl, tag)
}

func (u *Ufest) _load(csgl *memsys.SGL, tag string) error {
	data := csgl.Bytes()

	// validate and strip/remove the trailing checksum
	checksumOffset := len(data) - cos.SizeXXHash64
	expectedChecksum := binary.BigEndian.Uint64(data[checksumOffset:])
	compressedData := data[:checksumOffset]
	actualChecksum := onexxh.Checksum64S(compressedData, cos.MLCG32)
	if expectedChecksum != actualChecksum {
		return cos.NewErrMetaCksum(expectedChecksum, actualChecksum, utag)
	}

	givenID := u.id

	// decompress into a 2nd buffer
	dbuf, dslab := g.pmm.AllocSize(sizeLoad)
	err := u._decompress(compressedData, dbuf)
	dslab.Free(dbuf)

	if err != nil {
		e := fmt.Errorf("failed to load %s %s: %v", tag, u._utag(u.lom.Cname()), err)
		return cmn.NewErrLmetaCorrupted(e)
	}
	debug.Assert(u.validNums() == nil)

	if givenID != "" && givenID != u.id {
		return fmt.Errorf("loaded %s %s has different ID: given %q vs stored %q", tag, u._utag(u.lom.Cname()), givenID, u.id)
	}
	debug.AssertNoErr(cos.ValidateManifestID(u.id))
	return err
}

func (u *Ufest) _decompress(compressedData, buf []byte) error {
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
			return fmt.Errorf("failed to decompress: %w", err)
		}
	}

	if err := u.unpack(data); err != nil {
		return fmt.Errorf("failed to unpack: %w", err)
	}
	return nil
}

const _atid = ", id="

func (u *Ufest) _errLoad(tag, cname string, err error) error {
	return fmt.Errorf("failed to load %s %s: %w", tag, u._utag(cname), err)
}

func (u *Ufest) _utag(cname string) string {
	return utag + "[" + cname + _atid + u.id + "]"
}

func (u *Ufest) _itag(cname string) string {
	return itag + "[" + cname + _atid + u.id + "]"
}

func (u *Ufest) _rtag() string {
	var cname string
	if u.lom != nil {
		cname = u.lom.Cname()
	}
	return "chunked content[" + cname + _atid + u.id + "]"
}

func (u *Ufest) storeCompleted(lom *LOM) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// validate
	if err := u._errCompleted(lom); err != nil {
		return err
	}
	if u.count == 0 || int(u.count) != len(u.chunks) {
		return fmt.Errorf("%s: num %d vs %d", u._itag(lom.Cname()), u.count, len(u.chunks))
	}

	lsize := lom.Lsize(true)
	if u.size != 0 && u.size != lsize {
		return fmt.Errorf("%s store size: %d vs %d", u._itag(lom.Cname()), u.size, lsize)
	}

	var total int64
	for i := range u.count {
		total += u.chunks[i].size
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
	orig := c.path
	if err := lom.RenameFinalize(c.path); err != nil {
		return err
	}
	c.path = lom.FQN

	// store
	u.flags |= flCompleted
	u.completed.Store(true)

	sgl := g.pmm.NewSGL(u._packSize())

	debug.Assert(c.path == lom.FQN)
	err := u._store(lom, sgl, true)
	sgl.Free()

	if err == nil {
		partial := u._fqns(lom, false)
		err = cos.RemoveFile(partial)
		debug.AssertNoErr(err)
		return nil
	}

	// undo
	u.flags &^= flCompleted // undo
	u.completed.Store(false)
	if nerr := cos.Rename(lom.FQN, orig); nerr != nil {
		nlog.Errorf("failed to store %s: w/ nested error [%v, %v]", u._itag(lom.Cname()), err, nerr)
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	} else {
		c.path = orig
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
	for i := range u.chunks {
		c := &u.chunks[i]
		if c.num <= 0 || c.num > u.count {
			return fmt.Errorf("chunk %d has invalid number (%d/%d)", i, c.num, u.count)
		}
		for j := range i {
			if u.chunks[j].num == c.num {
				return fmt.Errorf("duplicate chunk number %d at positions %d and %d", c.num, i, j)
			}
		}
	}
	return nil
}

func (u *Ufest) StorePartial(lom *LOM, locked bool) error {
	if !locked {
		u.mu.Lock()
		defer u.mu.Unlock()
	}
	if err := u._errCompleted(lom); err != nil {
		return err
	}
	if err := cos.ValidateManifestID(u.id); err != nil {
		return fmt.Errorf("partial %s: %v", u._utag(lom.Cname()), err)
	}
	if u.count == 0 {
		return fmt.Errorf("partial %s must have at least one chunk", u._utag(lom.Cname()))
	}

	u.flags &^= flCompleted // always incomplete here
	sgl := g.pmm.NewSGL(u._packSize())
	err := u._store(lom, sgl, false)
	sgl.Free()
	return err
}

// estimating:
// - fixed portion: ~20 bytes
// - path: usually < 120 bytes
// - checksum value (wo/ type): ~16-32 bytes depending on algorithm
// - S3 fields: 0 to 50 bytes
func (u *Ufest) _packSize() int64 {
	estimated := int64(len(u.chunks)) * packedChunkSize
	return max(sizeStore, estimated+32)
}

func (u *Ufest) _store(lom *LOM, sgl *memsys.SGL, completed bool) error {
	if err := cos.ValidateManifestID(u.id); err != nil {
		return fmt.Errorf("cannot store %s: %v", u._utag(lom.Cname()), err)
	}

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

	return u.fwrite(lom, sgl, completed)
}

// note that Add() keeps chunks sorted by their respective numbers,
// so u.chunks[0] is the lowest present at any time
func (u *Ufest) firstChunk() (*Uchunk, error) {
	debug.AssertFunc(func() bool {
		for i := 1; i < len(u.chunks); i++ {
			if u.chunks[i-1].num > u.chunks[i].num {
				return false
			}
		}
		return true
	})

	if err := u.Check(); err != nil {
		return nil, err
	}
	return &u.chunks[0], nil
}

func (u *Ufest) Check() error {
	if len(u.chunks) == 0 {
		return errNoChunks
	}
	if u.chunks[0].num != 1 {
		return errNoChunkOne
	}
	return nil
}

//
// fread | fwrite
// (completed | partial)
//

func (u *Ufest) _fqns(lom *LOM, completed bool) string {
	if completed {
		return lom.GenFQN(fs.ChunkMetaCT)
	}
	debug.Func(func() {
		err := cos.ValidateManifestID(u.id)
		debug.AssertNoErr(err)
	})
	return lom.GenFQN(fs.ChunkMetaCT, u.id)
}

func (u *Ufest) fread(sgl *memsys.SGL, completed bool) error {
	debug.Assert(u.lom.IsLocked() > apc.LockNone, "expecting locked", u.lom.Cname())

	fqn := u._fqns(u.lom, completed)
	fh, err := os.Open(fqn)
	if err != nil {
		return err
	}
	defer cos.Close(fh)

	var n int64
	n, err = io.Copy(sgl, fh)
	if err != nil {
		return err
	}
	debug.Assert(n == sgl.Len())
	return nil
}

// locking rules:
// - there's always manifest mutex
// - if the caller is completing this manifest there must be LOM wlock
func (u *Ufest) fwrite(lom *LOM, sgl *memsys.SGL, completed bool) error {
	debug.Assert(u.Completed() == completed)
	debug.Assert(!completed || lom.IsLocked() == apc.LockWrite, "expecting w-locked", lom.Cname())

	fqn := u._fqns(lom, completed)
	wfh, err := cos.CreateFile(fqn)
	if err != nil {
		return err
	}
	defer cos.Close(wfh)

	var l, written int64
	l = sgl.Len()
	written, err = io.Copy(wfh, sgl)
	if err != nil {
		return err
	}
	if written != l {
		return fmt.Errorf("%s: invalid write size %d != %d", u._itag(lom.Cname()), written, l) // (unlikely)
	}
	return nil
}

//
// checksum and size of the _WHOLE_ ---------------------------------------------------------------
//

func (u *Ufest) ETagS3() (string, error) {
	debug.Assert(u.count > 0 && int(u.count) == len(u.chunks), "invalid chunks num ", u.count, " vs ", len(u.chunks))

	h := md5.New()
	for i := range u.count {
		c := &u.chunks[i]
		switch {
		case len(c.MD5) == md5.Size:
			h.Write(c.MD5)
			if c.ETag != "" {
				if bin, err := cmn.ETagToMD5(c.ETag); err == nil {
					debug.Assert(bytes.Equal(bin, c.MD5),
						"ETag/md5 mismatch: ", c.ETag, " vs ", cmn.MD5hashToETag(c.MD5))
				}
			}
		case c.ETag != "":
			bin, err := cmn.ETagToMD5(c.ETag)
			if err == nil && len(bin) == cos.LenMD5Hash {
				h.Write(bin)
				continue
			}
			fallthrough
		default:
			err := fmt.Errorf("%s: invalid ETag for chunk %d: %q", u._rtag(), i+1, c.ETag)
			debug.AssertNoErr(err)
			return "", err
		}
	}
	// S3 compliant multipart ETag has the following format:
	// (note: should there be a special case for u.count == 1?)
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)
	return cmn.MD5strToETag(s + cmn.AwsMultipartDelim + strconv.Itoa(int(u.count))), nil
}

// reread all chunk payloads to compute a checksum of the given type
// TODO: avoid the extra pass by accumulating during AddPart/StorePartial or by caching a tree-hash
// see also: s3/mpt for ListParts
func (u *Ufest) ComputeWholeChecksum(cksumH *cos.CksumHash) error {
	debug.Assert(u.count > 0 && int(u.count) == len(u.chunks), "invalid chunks num ", u.count, " vs ", len(u.chunks))

	c, err := u.firstChunk()
	if err != nil {
		return err
	}

	var written int64
	buf, slab := g.pmm.AllocSize(c.size)
	defer slab.Free(buf)

	for i := range u.count {
		c := &u.chunks[i]
		fh, err := os.Open(c.path)
		if err != nil {
			return err
		}
		nn, e := io.CopyBuffer(cksumH.H, fh, buf)
		cos.Close(fh)
		if e != nil {
			return e
		}
		if nn != c.size {
			return fmt.Errorf("%s chunk %d: invalid size: written %d, have %d", u._rtag(), c.num, nn, c.size)
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
	_packStr(w, u.id)

	// creation time
	binary.BigEndian.PutUint64(b64[:], uint64(u.created.UnixNano()))
	w.Write(b64[:])

	// number of chunks
	binary.BigEndian.PutUint16(b16[:], u.count)
	w.Write(b16[:])
	// flags
	binary.BigEndian.PutUint16(b16[:], u.flags)
	w.Write(b16[:])

	// chunks
	for _, c := range u.chunks {
		// chunk size
		binary.BigEndian.PutUint64(b64[:], uint64(c.size))
		w.Write(b64[:])

		// path
		_packStr(w, c.path)

		// checksum
		_packCksum(w, c.cksum)

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
	if u.id, offset, err = _unpackStr(data, offset); err != nil {
		return err
	}

	// start time
	if len(data) < offset+cos.SizeofI64 {
		return errors.New(tooShort)
	}
	timeNano := int64(binary.BigEndian.Uint64(data[offset:]))
	u.created = time.Unix(0, timeNano)
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
	u.chunks = make([]Uchunk, u.count)
	u.size = 0
	for i := range u.count {
		c := &u.chunks[i]

		// chunk size
		if len(data) < offset+cos.SizeofI64 {
			return errors.New(tooShort)
		}
		c.size = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += cos.SizeofI64
		u.size += c.size

		// chunk path
		if c.path, offset, err = _unpackStr(data, offset); err != nil {
			return err
		}

		// chunk checksum
		if c.cksum, offset, err = _unpackCksum(data, offset); err != nil {
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
		if l := len(c.MD5); l != 0 && l != cos.LenMD5Hash {
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

func (lom *LOM) CompleteUfest(u *Ufest) (err error) {
	lom.Lock(true)
	defer lom.Unlock(true)

	debug.AssertFunc(func() bool {
		var total int64
		for i := range u.count {
			total += u.chunks[i].size
		}
		return total == u.size
	})

	// Load LOM to determine if it was previously chunked and load old ufest for cleanup
	// NOTE: at this point, `lom` already holds updated ufest metadata and checksum that need to be persisted
	// Loading `lom` from disk would overwrite these. Need to clone and load it as another LOM variable instead
	var (
		prevLom   = lom.Clone()
		prevUfest *Ufest
	)
	if prevLom.Load(false /*cache it*/, true /*locked*/) == nil && prevLom.IsChunked() {
		prevUfest, err = NewUfest("", prevLom, true /*must-exist*/)
		debug.AssertNoErr(err)
		if err == nil {
			// Load old ufest for cleaning up old chunks after successful completion
			if errLoad := prevUfest.load(true); errLoad != nil {
				nlog.Errorln("failed to load previous", tagCompleted, prevUfest._utag(prevLom.Cname()), "err:", errLoad)
				prevUfest = nil
			}
		}
	}

	// ais versioning
	if lom.Bck().IsAIS() && lom.VersionConf().Enabled {
		if remSrc, ok := lom.GetCustomKey(cmn.SourceObjMD); !ok || remSrc == "" {
			lom.CopyVersion(prevLom)
			if err := lom.IncVersion(); err != nil {
				nlog.Errorln(err)
			}
		}
	}

	// (in the extremely unlikely case we need to rollback)
	var (
		wfqnMeta, wfqnFirst string
		completedFQN        string
	)
	if prevUfest != nil {
		completedFQN = prevLom.GenFQN(fs.ChunkMetaCT)
		debug.Assert(completedFQN == lom.GenFQN(fs.ChunkMetaCT))

		wfqnMeta = prevLom.GenFQN(fs.ChunkMetaCT, u.id)
		if _renameWarn(completedFQN, wfqnMeta) == nil {
			wfqnFirst = prevLom.GenFQN(fs.WorkCT, u.id)
			if _renameWarn(prevLom.FQN, wfqnFirst) != nil {
				_renameWarn(wfqnMeta, completedFQN)
				wfqnMeta, wfqnFirst = "", ""
			}
		}
	}

	lom.SetSize(u.size) // note: storeCompleted still performs non-debug validation
	if err := u.storeCompleted(lom); err != nil {
		u.Abort(lom)
		return err
	}
	lom.SetAtimeUnix(u.created.UnixNano())

	lom.setlmfl(lmflChunk)
	debug.Assert(lom.md.lid.haslmfl(lmflChunk))

	if err := lom.PersistMain(true /*isChunked*/); err != nil {
		if prevUfest == nil {
			lom.md.lid.clrlmfl(lmflChunk)
		} else if wfqnFirst != "" && wfqnMeta != "" {
			// rollback
			_renameWarn(wfqnFirst, prevLom.FQN)
			_renameWarn(wfqnMeta, completedFQN)
		}
		u.Abort(lom)
		return err
	}

	if prevUfest != nil {
		if wfqnMeta != "" {
			os.Remove(wfqnMeta)
		}
		if wfqnFirst != "" {
			os.Remove(wfqnFirst)
		}
		prevUfest.removeChunks(prevLom, true /*except first*/)
	}

	return nil
}

func _renameWarn(src, dst string) (err error) {
	if err = os.Rename(src, dst); err != nil {
		nlog.WarningDepth(1, "nested err:", err)
	}
	return
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

	u, e := NewUfest("", lom, true)
	debug.AssertNoErr(e)
	if err := u.LoadCompleted(lom); err != nil {
		return nil, err
	}
	return u.NewReader()
}

func (u *Ufest) NewReader() (*UfestReader, error) {
	if !u.Completed() {
		return nil, fmt.Errorf("%s: is incomplete - cannot be read", u._rtag())
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
		c := &u.chunks[r.cidx]

		// open on demand
		if r.cfh == nil {
			debug.Assert(r.coff == 0)
			r.cfh, err = os.Open(c.path)
			if err != nil {
				return n, fmt.Errorf("%s: failed to open chunk (%d/%d)", r.u._rtag(), r.cidx+1, u.count)
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
				return n, fmt.Errorf("%s: chunk (%d/%d) appears to be truncated (%d/%d): %v",
					r.u._rtag(), r.cidx+1, u.count, r.coff, c.size, io.ErrUnexpectedEOF)
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
	for ; idx < len(u.chunks) && chunkoff >= u.chunks[idx].size; idx++ {
		chunkoff -= u.chunks[idx].size
	}
	if idx >= len(u.chunks) {
		return 0, io.EOF
	}

	total := len(p)
	if total == 0 {
		return 0, nil
	}
	// read
	for n < total && idx < len(u.chunks) {
		c := &u.chunks[idx]
		debug.Assert(c.size-chunkoff > 0, c.size, " vs ", chunkoff)
		toRead := min(int64(total-n), c.size-chunkoff)
		fh, err := os.Open(c.path)
		if err != nil {
			return n, fmt.Errorf("%s: failed to open chunk (%d/%d)", r.u._rtag(), idx+1, u.count)
		}

		var m int
		m, err = fh.ReadAt(p[n:n+int(toRead)], chunkoff)
		cos.Close(fh)

		n += m
		if err != nil {
			return n, fmt.Errorf("%s: failed to read chunk (%d/%d) at offset %d: %v",
				r.u._rtag(), idx+1, u.count, chunkoff, err)
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
