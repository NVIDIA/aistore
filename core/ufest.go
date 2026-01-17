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
//   * Chunks (a.k.a., parts) may arrive **unordered**; duplicates are tolerated,
//     and the most recent copy of a given `partNumber` (chunk number) wins.
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
	// MaxChunkCount is the maximum number of chunks allowed per object (limited by uint16 chunk numbering)
	MaxChunkCount = 9999
)

const (
	sizeStore = memsys.MaxPageSlabSize // SGL buffer sizing for store

	// Estimated worst-case packed size per chunk:
	// - Fixed fields: 8 (size) + 2 (num) + 2 (flags) = 12 bytes
	// - Path: 2 + 256 (very long object/bucket names) = 258 bytes
	// - Checksum: 2+6 (type) + 2+128 (sha512 hex) = 138 bytes
	// - MD5: 2 + 16 = 18 bytes
	// - ETag: 2 + 100 (complex S3 multipart) = 102 bytes
	// Total per chunk: ~512 bytes
	estPackedChunkSize = 512

	// Buffer size for manifest load: must fit MaxChunkCount chunks
	sizeLoad = MaxChunkCount * estPackedChunkSize
)

// single flag so far
const (
	flCompleted uint16 = 1 << 0 // Ufest.flags
)

const (
	utag = "chunk-manifest"
	itag = "invalid " + utag

	tagCompleted = "completed"
	tagPartial   = "partial"
)

type (
	Uchunk struct {
		cksum *cos.Cksum // nil means none; otherwise non-empty
		path  string     // (may become v2 _remote location_)
		ETag  string     // S3/legacy
		MD5   []byte     // ditto
		size  int64      // this chunk size
		num   uint16     // chunk/part number
		flags uint16     // bit flags (compression; future use)
	}
	Ufest struct {
		created time.Time // creation time
		lom     *LOM      // parent lom (runtime)
		id      string    // upload/manifest ID
		chunks  []Uchunk  // all chunks

		size      int64       // total
		mu        sync.Mutex  // protect
		completed atomic.Bool // in-sync with lmflChunk
		count     uint16      // number of chunks (so far)
		flags     uint16      // bit flags { completed, ...}
	}
)

var (
	errNoChunks = errors.New("no chunks")
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
// - arg `num` is chunk.num (starts from 1)
// - chunk #1 stays on the object's mountpath - see chunk1Path()
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
	if num <= 0 {
		return nil, fmt.Errorf("%s: invalid chunk number (%d)", u._utag(lom.Cname()), num)
	}
	if num > MaxChunkCount {
		return nil, fmt.Errorf("%s: chunk number (%d) exceeds the maximum allowed (%d)", u._utag(lom.Cname()), num, MaxChunkCount)
	}

	// note first chunk's location
	if num == 1 {
		return &Uchunk{
			path: u.chunk1Path(lom, false /*completed*/),
			num:  1,
		}, nil
	}

	// all the rest chunks
	return u.newChunk2N(num, lom)
}

// effectively, "%04d"
// >=10000 uses raw num (update when needed)
func formatCnum(num int) string {
	if uint(num) >= 10000 {
		return strconv.Itoa(num)
	}
	var b [4]byte
	b[3] = byte('0' + num%10)
	num /= 10
	b[2] = byte('0' + num%10)
	num /= 10
	b[1] = byte('0' + num%10)
	num /= 10
	b[0] = byte('0' + num%10)
	return string(b[:])
}

// check whether completed chunks are properly located
func (u *Ufest) IsHRW(avail fs.MPI) (bool, error) {
	debug.Assert(u.lom.IsHRW(), "must be already checked by the caller")
	debug.Assert(u.Completed(), "ditto (don't call with partial)")
	for i := range u.chunks {
		c := &u.chunks[i]
		switch c.num {
		case 1:
			if c.path != u.lom.FQN {
				return false, nil
			}
		default:
			hrwkey := u.id + formatCnum(int(c.num))
			mi, _ /*digest*/, err := avail.Hrw(cos.UnsafeB(hrwkey))
			if err != nil {
				return false, err
			}
			if !mi.HasPath(c.path) {
				return false, nil
			}
		}
	}
	return true, nil
}

// (note fs.Hrw => fs.GetAvail())
func (u *Ufest) newChunk2N(num int, lom *LOM) (*Uchunk, error) {
	snum := formatCnum(num)
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

// (when manifest may be shared: locking is caller's responsibility)
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

	// add
	err := u._add(c)
	if err != nil {
		u.mu.Unlock()
		return err
	}
	// store
	if n := lom.Bprops().Chunks.CheckpointEvery; n > 0 {
		if u.count%uint16(n) == 0 {
			err = u.StorePartial(lom, true)
		}
	}
	u.mu.Unlock()
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

// (when manifest may be shared: locking is caller's responsibility)
func (u *Ufest) GetChunk(num int) (*Uchunk, error) {
	debug.Assert(num > 0, num)
	l := len(u.chunks)
	if l == 0 {
		return nil, errNoChunks
	}
	if num <= l {
		c := &u.chunks[num-1]
		if c.num == uint16(num) {
			return c, nil
		}
	}
	for i := range l {
		c := &u.chunks[i]
		if c.num == uint16(num) {
			return c, nil
		}
	}
	return nil, cos.NewErrNotFound(u.lom, "chunk "+strconv.Itoa(num))
}

func (u *Ufest) removeChunks(lom *LOM, exceptFirst bool) {
	var (
		ecnt int
		skip uint16
	)
	if exceptFirst {
		c := u.firstChunk()
		skip = c.num
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

// load the given LOM's manifest into memory
// 1) find and read the manifest file from file system based on the given LOM's FQN
// 2) decompress the manifest data using LZ4 and deserialize it into the `Ufest` struct
// 3) validate the compressed data checksum
func (u *Ufest) load(completed bool) error {
	csgl := g.pmm.NewSGL(sizeLoad)
	defer csgl.Free()

	tag := cos.Ternary(completed, tagCompleted, tagPartial)
	if err := u.fread(csgl, completed); err != nil {
		return u._errLoad(tag, u.lom.Cname(), err)
	}
	if err := u._load(csgl); err != nil {
		return u._errLoad(tag, u.lom.Cname(), err)
	}
	if err := u.Check(completed); err != nil {
		return u._errLoad(tag, u.lom.Cname(), cmn.NewErrLmetaCorrupted(err))
	}
	return nil
}

func (u *Ufest) _load(csgl *memsys.SGL) error {
	totalSize := csgl.Size()
	if totalSize < int64(cos.SizeXXHash64) {
		return fmt.Errorf("manifest too short: %d bytes", totalSize)
	}

	checksumOffset := totalSize - int64(cos.SizeXXHash64)
	givenID := u.id

	// Compressed data (excluding trailing checksum)
	compressedReader := io.LimitReader(csgl, checksumOffset)

	// Calculate checksum WHILE decompressing using TeeReader
	h := onexxh.NewS64(cos.MLCG32)
	err := u._decompress(io.TeeReader(compressedReader, h))
	if err != nil {
		return cmn.NewErrLmetaCorrupted(err)
	}

	return u._validate(csgl, h, givenID)
}

func (u *Ufest) _decompress(compressedReader io.Reader) error {
	// hold decompressed data
	sgl := g.pmm.NewSGL(estPackedChunkSize * iniChunksCap)
	defer sgl.Free()

	zr := lz4.NewReader(compressedReader)
	if _, err := io.Copy(sgl, zr); err != nil {
		return fmt.Errorf("failed to decompress: %w", err)
	}

	debug.Assert(sgl.Size() > 0, "decompressed manifest is empty")

	if err := u.unpack(sgl); err != nil {
		return fmt.Errorf("failed to unpack: %w", err)
	}

	return nil
}

func (u *Ufest) _validate(csgl io.Reader, h *onexxh.XXHash64, givenID string) error {
	// Read the expected checksum from last 8 bytes
	var checksumBuf [cos.SizeXXHash64]byte
	if _, err := io.ReadFull(csgl, checksumBuf[:]); err != nil {
		return fmt.Errorf("failed to read expected checksum: %w", err)
	}
	expectedChecksum := binary.BigEndian.Uint64(checksumBuf[:])

	actualChecksum := h.Sum64()
	debug.Assert(expectedChecksum != 0, "expected checksum is zero")
	debug.Assert(actualChecksum != 0, "actual checksum is zero")

	if expectedChecksum != actualChecksum {
		return cos.NewErrMetaCksum(expectedChecksum, actualChecksum, utag)
	}

	if givenID != "" && givenID != u.id {
		return fmt.Errorf("loaded %s has different ID: given %q vs stored %q", u._utag(u.lom.Cname()), givenID, u.id)
	}
	debug.AssertNoErr(cos.ValidateManifestID(u.id))

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

func (u *Ufest) storeCompleted(lom *LOM, overrideCompleted bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// validate
	if !overrideCompleted {
		if err := u._errCompleted(lom); err != nil {
			return err
		}
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

	if err := u.Check(true /*completed*/); err != nil {
		return fmt.Errorf("%s: failed to store, err: %v", u._itag(lom.Cname()), err)
	}

	// fixup chunk #1
	c := u.firstChunk()
	orig := c.path
	if err := lom.RenameFinalize(c.path); err != nil {
		return err
	}
	c.path = u.chunk1Path(lom, true /*completed*/)

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
		debug.AssertNoErr(u.ValidateChunkLocations(true))
	}
	return err
}

func (u *Ufest) _errCompleted(lom *LOM) error {
	if u.Completed() {
		return errors.New(u._itag(lom.Cname()) + ": already completed")
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
	if err := u.Check(false /*completed*/); err != nil {
		return fmt.Errorf("%s: failed to store partial, err: %v", u._itag(lom.Cname()), err)
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
	estimated := int64(len(u.chunks)) * estPackedChunkSize
	return max(sizeStore, estimated+32)
}

func (u *Ufest) _store(lom *LOM, sgl *memsys.SGL, completed bool) error {
	if err := cos.ValidateManifestID(u.id); err != nil {
		return fmt.Errorf("cannot store %s: %v", u._utag(lom.Cname()), err)
	}

	// checksum while compressing
	h := onexxh.NewS64(cos.MLCG32)
	mw := io.MultiWriter(sgl, h)
	zw := lz4.NewWriter(mw)
	u.pack(zw)
	zw.Close()

	debug.Assert(sgl.Size() > 0, "compressed manifest is empty")

	// Write trailing checksum
	checksum := h.Sum64()
	debug.Assert(checksum != 0, "manifest checksum is zero")

	var checksumBuf [cos.SizeXXHash64]byte
	binary.BigEndian.PutUint64(checksumBuf[:], checksum)
	sgl.Write(checksumBuf[:])

	return u.fwrite(lom, sgl, completed)
}

//
// chunk validations
//

func (u *Ufest) Check(completed bool) error {
	if len(u.chunks) == 0 {
		return errNoChunks
	}

	// (same as target's MPU complete() --> _checkParts())
	if completed {
		if len(u.chunks) != int(u.count) {
			return fmt.Errorf("chunk counting error: count=%d vs len=%d", u.count, len(u.chunks))
		}
		for i := range u.count {
			c := &u.chunks[i]
			if c.num != i+1 {
				return fmt.Errorf("chunk at (%d) has invalid number %d (count=%d)", i, c.num, u.count)
			}
		}
		return nil
	}

	for i := 1; i < len(u.chunks); i++ {
		a, b := &u.chunks[i-1], &u.chunks[i]
		if a.num == 0 || a.num > MaxChunkCount {
			return fmt.Errorf("chunk at (%d) has invalid number %d", i, a.num)
		}
		if a.num >= b.num {
			return fmt.Errorf("chunks are not ascending in numbers: (%d: %d) vs (%d: %d)",
				i-1, a.num, i, b.num)
		}
	}
	return nil
}

// note: used only in tests and asserts
func (u *Ufest) ValidateChunkLocations(completed bool) error {
	l := len(u.chunks)
	if l == 0 {
		return errNoChunks
	}
	if completed && !u.Completed() {
		return errors.New("expecting completed manifest")
	}
	for i := range l {
		num := i + 1
		c, err := u.GetChunk(num)
		if err != nil {
			return err
		}
		var exp string
		if c == u.firstChunk() {
			exp = u.chunk1Path(u.lom, completed)
		} else {
			b, err := u.newChunk2N(num, u.lom)
			if err != nil {
				return err
			}
			exp = b.path
		}
		if c.path != exp {
			return fmt.Errorf("chunk at (%d) has invalid location: expecting %q, got %q", i, exp, c.path)
		}
	}
	return nil
}

//
// helpers for: first chunk and chunk #1
//

// return the lowest currently present (note: u.Add() keeps sorted order)
func (u *Ufest) firstChunk() *Uchunk {
	debug.Assert(len(u.chunks) > 0)
	return &u.chunks[0]
}

// return the path chunk #1 must have
func (u *Ufest) chunk1Path(lom *LOM, completed bool) string {
	if completed {
		return lom.FQN
	}
	return lom.GenFQN(fs.ChunkCT, u.id, formatCnum(1))
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
						"ETag/md5 mismatch: ", c.ETag, " vs ", cmn.MD5ToQuotedETag(c.MD5))
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
	// note:
	// - returning with `"` quotes
	// - should there be a special case for u.count == 1?
	sum := h.Sum(nil)
	s := hex.EncodeToString(sum)
	return cmn.QuoteETag(s + cmn.AwsMultipartDelim + strconv.Itoa(int(u.count))), nil
}

// reread all chunk payloads to compute a checksum of the given type
// see also: s3/mpt for ListParts
// TODO: optimize-out; callers to revise locking
func (u *Ufest) ComputeWholeChecksum(cksumH *cos.CksumHash) error {
	debug.AssertNoErr(u.Check(true /*completed*/))
	const tag = "[whole-checksum]"
	var (
		written   int64
		c         = u.firstChunk()
		buf, slab = g.pmm.AllocSize(c.size)
	)
	defer slab.Free(buf)

	for i := range u.count {
		c := &u.chunks[i]
		fh, err := os.Open(c.path)
		if err != nil {
			if cos.IsNotExist(err) {
				if e := u.lom.Load(false, true); e != nil {
					err = e
				}
			}
			return fmt.Errorf("%s %s chunk %d: %w", tag, u._rtag(), c.num, err)
		}
		nn, e := io.CopyBuffer(cksumH.H, fh, buf)
		cos.Close(fh)
		if e != nil {
			return e
		}
		if nn != c.size {
			return fmt.Errorf("%s %s chunk %d: invalid size: written %d, have %d", tag, u._rtag(), c.num, nn, c.size)
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
	debug.Assert(int(u.count) == len(u.chunks))
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

func (u *Ufest) unpack(r io.Reader) (err error) {
	var (
		buf8 [cos.SizeofI64]byte // buffer for 64-bit reads
		buf2 [cos.SizeofI16]byte // buffer for 16-bit reads
	)

	// meta-version
	if _, err = io.ReadFull(r, buf2[:1]); err != nil {
		return fmt.Errorf("failed to read meta-version: %w", err)
	}
	if buf2[0] != umetaver {
		return fmt.Errorf("unsupported %s meta-version %d (expecting %d)", utag, buf2[0], umetaver)
	}

	// upload ID
	if u.id, err = _unpackStr(r, buf2[:]); err != nil {
		return err
	}

	// start time
	if _, err = io.ReadFull(r, buf8[:]); err != nil {
		return fmt.Errorf("failed to read creation time: %w", err)
	}
	timeNano := int64(binary.BigEndian.Uint64(buf8[:]))
	u.created = time.Unix(0, timeNano)

	// number of chunks
	if _, err = io.ReadFull(r, buf2[:]); err != nil {
		return fmt.Errorf("failed to read chunk count: %w", err)
	}
	u.count = binary.BigEndian.Uint16(buf2[:])

	// flags
	if _, err = io.ReadFull(r, buf2[:]); err != nil {
		return fmt.Errorf("failed to read flags: %w", err)
	}
	u.flags = binary.BigEndian.Uint16(buf2[:])

	// Read chunks
	u.chunks = make([]Uchunk, u.count)
	u.size = 0
	for i := range u.count {
		c := &u.chunks[i]

		// chunk size
		if _, err = io.ReadFull(r, buf8[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d size: %w", i, err)
		}
		c.size = int64(binary.BigEndian.Uint64(buf8[:]))
		u.size += c.size

		// chunk path
		if c.path, err = _unpackStr(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d path: %w", i, err)
		}

		// chunk checksum
		if c.cksum, err = _unpackCksum(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d checksum: %w", i, err)
		}

		// chunk number
		if _, err = io.ReadFull(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d number: %w", i, err)
		}
		c.num = binary.BigEndian.Uint16(buf2[:])

		// chunk flags
		if _, err = io.ReadFull(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d flags: %w", i, err)
		}
		c.flags = binary.BigEndian.Uint16(buf2[:])

		// S3 legacy - MD5
		if c.MD5, err = _unpackBytes(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d MD5: %w", i, err)
		}
		if l := len(c.MD5); l != 0 && l != cos.LenMD5Hash {
			return fmt.Errorf("invalid MD5 size %d", l)
		}

		// S3 legacy - ETag
		if c.ETag, err = _unpackStr(r, buf2[:]); err != nil {
			return fmt.Errorf("failed to read chunk %d ETag: %w", i, err)
		}
	}
	return nil
}

func _unpackStr(r io.Reader, buf2 []byte) (s string, err error) {
	var b []byte
	b, err = _unpackBytes(r, buf2)
	if err == nil {
		s = string(b)
	}
	return
}

func _unpackBytes(r io.Reader, buf2 []byte) ([]byte, error) {
	// Read 2-byte length
	if _, err := io.ReadFull(r, buf2); err != nil {
		return nil, fmt.Errorf("failed to read length: %w", err)
	}

	l := int(binary.BigEndian.Uint16(buf2))
	if l == 0 {
		return nil, nil
	}

	// Read the actual bytes
	data := make([]byte, l)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("failed to read %d bytes: %w", l, err)
	}
	return data, nil
}

func _unpackCksum(r io.Reader, buf2 []byte) (cksum *cos.Cksum, err error) {
	var (
		ty, val string
	)
	ty, err = _unpackStr(r, buf2)
	if err != nil || ty == "" {
		return nil, err
	}
	val, err = _unpackStr(r, buf2)
	if err == nil {
		cksum = cos.NewCksum(ty, val)
	}
	return
}

//
// LOM: chunk persistence ------------------------------------------------------
//

func (lom *LOM) CompleteUfest(u *Ufest, locked bool) (err error) {
	if !locked {
		lom.Lock(true)
		defer lom.Unlock(true)
	}

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
		if err == nil {
			// Load old ufest for cleaning up old chunks after successful completion
			errLoad := prevUfest.load(true)
			if errLoad != nil {
				if !cos.IsNotExist(errLoad) {
					nlog.Errorln("failed to load previous", tagCompleted, prevUfest._utag(prevLom.Cname()), "err:", errLoad)
				}
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

	lom.SetSize(u.size)
	if err := u.storeCompleted(lom, false /*override*/); err != nil {
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

// Relocate a chunked object to its canonical location(s) without making extra copies;
// must be called under LOM write-lock.
// Relocate is a single-threaded operation: the caller must NOT share this Ufest instance
// across goroutines.
// See also:
// - lom.Copy2FQN()

type (
	// work
	moveRec struct {
		src     string
		dst     string
		dstWork string
		chunk   *Uchunk
	}
	moveRecs []moveRec
	// result
	moveRes struct {
		err        error
		oldPaths   []string
		cnt        int
		firstMoved bool
	}
)

func (recs moveRecs) cleanup() {
	for i := range recs {
		_ = cos.RemoveFile(recs[i].dstWork)
	}
}

func (u *Ufest) Relocate(hrwMi *fs.Mountpath, buf []byte) (*LOM, error) {
	lom := u.lom

	debug.Assertf(lom.IsLocked() == apc.LockWrite, "%s must be w-locked (have %d)", lom.Cname(), lom.IsLocked())
	debug.Assert(lom.IsChunked())

	if err := u.Check(true /*completed*/); err != nil {
		return nil, err
	}

	var (
		oldManifestFQN = lom.GenFQN(fs.ChunkMetaCT)
		dstObjFQN      = hrwMi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
	)

	res := u.reloc(hrwMi, dstObjFQN, buf)
	if res.err != nil {
		return nil, res.err
	}

	// nothing to do
	if !res.firstMoved && res.cnt == 0 {
		return lom, nil
	}

	// only chunks 2..N moved: re-persist manifest with updated paths
	if !res.firstMoved {
		if err := u.storeCompleted(lom, true /*override*/); err != nil {
			return nil, err
		}
		u.relocCleanup(res.oldPaths)
		return lom, nil
	}

	// chunk #1 moved: initialize hlom at canonical location
	hlom := &LOM{}
	if err := hlom.InitFQN(dstObjFQN, lom.Bucket()); err != nil {
		return nil, err
	}
	debug.Assert(hlom.Mountpath().Path == hrwMi.Path, hlom.Mountpath().Path, " vs ", hrwMi.Path)
	hlom.CopyAttrs(lom, false /*skip checksum*/)

	// persist completed manifest at new location
	u.lom = hlom // ostensibly, to pass assert(validate-locations)
	if err := u.storeCompleted(hlom, true /*override*/); err != nil {
		return nil, err
	}

	// persist parent LOM
	hlom.SetSize(u.size)
	hlom.setlmfl(lmflChunk)
	if err := hlom.PersistMain(true /*chunked*/); err != nil {
		return nil, err
	}

	// cleanup (old chunks + old manifest)
	u.relocCleanup(res.oldPaths)
	if err := cos.RemoveFile(oldManifestFQN); err != nil {
		nlog.Warningf("%s: failed to remove old manifest %q: %v", lom.Cname(), oldManifestFQN, err)
	}

	return hlom, nil
}

func (u *Ufest) reloc(hrwMi *fs.Mountpath, dstObjFQN string, buf []byte) moveRes {
	var (
		dstlom = &LOM{}
		recs   = make(moveRecs, 0, u.count)
	)
	if err := dstlom.InitFQN(dstObjFQN, u.lom.Bucket()); err != nil {
		return moveRes{err: err}
	}

	// 1) build recs
	for i := range u.chunks {
		chunk := &u.chunks[i]

		// resolve current chunk path => mountpath
		var parsed fs.ParsedFQN
		if err := parsed.Init(chunk.path); err != nil {
			return moveRes{err: err}
		}

		// compute destination path
		var dst string
		switch chunk.num {
		case 1:
			debug.Assert(i == 0, "expecting first chunk at index 0")
			if parsed.Mountpath.Path == hrwMi.Path {
				if chunk.path != dstObjFQN {
					// (unlikely)
					err := fmt.Errorf("%s: chunk #1 path mismatch: have %q, want %q", u.lom.Cname(), chunk.path, dstObjFQN)
					debug.AssertNoErr(err)
					return moveRes{err: err}
				}
				continue // nothing to do
			}
			dst = dstObjFQN
		default:
			c, err := u.newChunk2N(int(chunk.num), dstlom)
			if err != nil {
				return moveRes{err: err}
			}
			if c.path == chunk.path {
				continue // nothing to do
			}
			dst = c.path
		}

		dstWork := dst + ".reloc." + cos.GenTie()
		recs = append(recs, moveRec{src: chunk.path, dst: dst, dstWork: dstWork, chunk: chunk})
	}

	if len(recs) == 0 {
		return moveRes{} // nothing to do
	}

	// 2) copy all to dstWork; rollback on failure
	for i := range recs {
		rec := &recs[i]
		if _, _, err := cos.CopyFile(rec.src, rec.dstWork, buf, cos.ChecksumNone); err != nil {
			recs[:i].cleanup()
			return moveRes{err: err}
		}
	}

	// 3) finalize
	for i := range recs {
		rec := &recs[i]
		_ = cos.RemoveFile(rec.dst)
		if err := os.Rename(rec.dstWork, rec.dst); err != nil {
			// rollback: remove already-published dst files + remaining work files
			for k := range i {
				if e := cos.RemoveFile(recs[k].dst); e != nil {
					nlog.Warningln("rollback:", u.lom.Cname(), e)
				}
			}
			recs[i:].cleanup()
			return moveRes{err: err}
		}
	}

	// 4) update in-memory chunk paths; collect old paths
	var (
		res    = moveRes{oldPaths: make([]string, 0, len(recs))}
		except = make(cos.StrSet, len(recs))
	)
	for i := range recs {
		except.Set(recs[i].dst) // (not to delete)
	}
	for i := range recs {
		rec := &recs[i]
		debug.Assert(rec.src != rec.dst)
		if !except.Contains(rec.src) {
			res.oldPaths = append(res.oldPaths, rec.src)
		}

		rec.chunk.path = rec.dst
		if rec.chunk.num == 1 {
			res.firstMoved = true
		}
	}
	res.cnt = len(recs)
	return res
}

func (*Ufest) relocCleanup(oldPaths []string) {
	for _, p := range oldPaths {
		if err := cos.RemoveFile(p); err != nil {
			nlog.Warningf("reloc cleanup: failed to remove %q: %v", p, err)
		}
	}
}
