// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"math/bits"
	"slices"
	"strings"

	onexxh "github.com/OneOfOne/xxhash"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
)

// Binary layout of a packed ShardIndex:
//
// ┌─────────────────────────────────────────────────────────────────────────┐
// │  PREAMBLE (11 bytes, fixed)                                             │
// │  [0]      meta-version uint8   - shardIdxMetaver                        │
// │  [1]      format       uint8   - 0 = TAR (shardIdxFmtTAR)               │
// │  [2]      cksum type   uint8   - 1 = onexxh (shardIdxCksumXXH)          │
// │  [3..10]  xxhash64     uint64  - checksum of preamble kind + payload    │
// ├─────────────────────────────────────────────────────────────────────────┤
// │  PAYLOAD (variable, covered by xxhash)                                  │
// │  src_cksum_type_len  uvarint - byte length of LOM cksum type            │
// │  src_cksum_type      []byte  - e.g. "xxhash" (UTF-8)                    │
// │  src_cksum_val_len   uvarint - byte length of LOM cksum value           │
// │  src_cksum_val       []byte  - hex-encoded value                        │
// │  src_size            uvarint - LOM size in bytes                        │
// │  count    uvarint              - number of entries                      │
// │  for each entry (sorted by name - see Lookup):                          │
// │    name_len  uvarint           - byte length of the name string         │
// │    name      []byte            - UTF-8 file path                        │
// │    offset    uvarint           - byte offset of the TAR header block    │
// │    size      uvarint           - logical file size in bytes             │
// └─────────────────────────────────────────────────────────────────────────┘
//
// This indexer supports regular-file members from the common TAR variants handled by Go's stdlib,
// including long-name PAX/GNU cases. We intentionally skip sparse (tar.TypeGNUSparse)
// and non-regular entries (directories, links, device nodes, FIFOs).

const (
	shardIdxMetaver   = 2 // current meta-version
	shardIdxMetaverV1 = 1 // [backward compatibility] previous supported meta-version
)
const (
	shardIdxFmtTAR   = 0  // format: TAR
	shardIdxCksumXXH = 1  // checksum: onexxh.Checksum64S with cos.MLCG32 seed
	shardIdxPrefLen  = 11 // [1:ver | 1:fmt | 1:cksum-type | 8:xxhash64]

	ShardIdxMinLen     = shardIdxPrefLen + 4 // preamble + four single-byte uvarints (lower bound)
	shardIdxMaxEntries = 1 << 20             // the cap to bound memory
)

type (
	ShardIndexEntry struct {
		// Byte offset of the 512-byte header block immediately preceding the file's data
		// (i.e. DataOffset() - TarBlockSize). Always a multiple of TarBlockSize; the first
		// entry in a shard can be at offset 0.
		// For PAX/GNU long names the member begins earlier, at the extended-header record carrying
		// the real name.
		Offset int64

		// File size in bytes (as recorded in the TAR header).
		Size int64
	}
	ShardIndex struct {
		// The shard's checksum and size as of index-build time.
		srcCksum cos.Cksum
		srcSize  int64

		// The index is held in its packed (on-disk) form and read in place:
		// * buf  - preamble + payload
		// * offs - byte offset of each entry within buf, strictly ordered by entry name
		buf  []byte
		offs []uint32

		bufSlab, offsSlab     *memsys.Slab
		bufPooled, offsPooled *shardIdxBuf
	}
)

// private
type (
	// scan-time (name, entry) pair; exists only between BuildShardIndex and pack
	_shentry struct {
		name string
		e    ShardIndexEntry
	}
	// cursor for sequential decoding
	idxDecoder struct {
		b   []byte
		off int
	}
)

var (
	ErrShardIdxStale   = errors.New("shard index: stale")
	ErrShardIdxCorrupt = errors.New("shard index: corrupted")
)

func _emitErr(format string, a ...any) error {
	return fmt.Errorf("shard index: "+format, a...)
}
func _corruptErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrShardIdxCorrupt}, a...)...)
}
func _staleErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrShardIdxStale}, a...)...)
}

// BuildShardIndex builds an in-memory index; the caller must call Free when done.
func BuildShardIndex(r io.ReaderAt, srcSize int64, srcCksum *cos.Cksum) (*ShardIndex, error) {
	const (
		minCap = 8
		maxCap = 64 * 1024
	)
	var (
		sr   = io.NewSectionReader(r, 0, srcSize)
		tr   = tar.NewReader(sr)
		ents = make([]_shentry, 0, cos.ClampInt(int(srcSize/TarBlockSize), minCap, maxCap))
	)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, _emitErr("tar reader failure: %w", err)
		}
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			// regular file - index below
		case tar.TypeGNUSparse:
			continue // not indexed: logical size != physical; caller falls back to sequential scan
		default:
			continue // skip directories, symlinks, devices, etc.
		}
		if len(ents) >= shardIdxMaxEntries {
			return nil, _emitErr("too many entries (max %d)", shardIdxMaxEntries)
		}
		dataOffset, _ := sr.Seek(0, io.SeekCurrent)
		debug.Func(func() { debug.Assert(dataOffset&(TarBlockSize-1) == 0, dataOffset) })

		ents = append(ents, _shentry{
			name: hdr.Name,
			e: ShardIndexEntry{
				Offset: dataOffset - TarBlockSize, // the 512-byte file header, not the data
				Size:   hdr.Size,
			},
		})
	}

	// offset order
	slices.SortFunc(ents, func(a, b _shentry) int {
		if c := strings.Compare(a.name, b.name); c != 0 {
			return c
		}
		return cmp.Compare(a.e.Offset, b.e.Offset)
	})

	if srcCksum == nil {
		srcCksum = cos.NoneCksum
	}
	idx := &ShardIndex{srcCksum: *srcCksum, srcSize: srcSize}
	if err := idx.pack(ents); err != nil {
		return nil, err
	}
	return idx, nil
}

// TODO: move it out of prod.
func NewShardIndexTestOnly(srcCksum *cos.Cksum, srcSize int64, entries map[string]ShardIndexEntry) (*ShardIndex, error) {
	ents := make([]_shentry, 0, len(entries))
	for name, e := range entries {
		ents = append(ents, _shentry{name: name, e: e})
	}
	// map keys are unique - no need for a stable sort here
	slices.SortFunc(ents, func(a, b _shentry) int { return strings.Compare(a.name, b.name) })

	if srcCksum == nil {
		srcCksum = cos.NoneCksum
	}
	idx := &ShardIndex{srcCksum: *srcCksum, srcSize: srcSize}
	if err := idx.pack(ents); err != nil {
		return nil, err
	}
	return idx, nil
}

// TODO: ditto
func (idx *ShardIndex) AllTestOnly() iter.Seq2[string, ShardIndexEntry] {
	return func(yield func(string, ShardIndexEntry) bool) {
		for _, off := range idx.offs {
			name, e := idx.at(off)
			if !yield(name, e) {
				return
			}
		}
	}
}

// number of bytes binary.PutUvarint writes for x
func uvarintLen(x uint64) int { return (bits.Len64(x|1) + 6) / 7 }

// Metaver 2:
// - fold the three preamble bytes into otherwise-unused high bits of the seed
func shardIdxHash(b []byte) uint64 {
	seed := uint64(cos.MLCG32) |
		uint64(b[0])<<32 |
		uint64(b[1])<<40 |
		uint64(b[2])<<48
	return onexxh.Checksum64S(b[shardIdxPrefLen:], seed)
}

////////////////
// ShardIndex //
////////////////

// serialize into the packed form and record each entry's byte offset
func (idx *ShardIndex) pack(ents []_shentry) error {
	if idx.srcSize < 0 {
		return _emitErr("negative src size %d", idx.srcSize)
	}
	debug.Func(func() { err := idx.srcCksum.Validate(); debug.AssertNoErr(err) })
	cksumTy, cksumVal := idx.srcCksum.Get()

	// first-wins on duplicates (if any)
	ents = slices.CompactFunc(ents, func(a, b _shentry) bool { return a.name == b.name })

	n := len(ents)
	total := shardIdxPrefLen +
		uvarintLen(uint64(len(cksumTy))) + len(cksumTy) +
		uvarintLen(uint64(len(cksumVal))) + len(cksumVal) +
		uvarintLen(uint64(idx.srcSize)) +
		uvarintLen(uint64(n))
	for i := range ents {
		e := &ents[i]
		if e.e.Offset < 0 {
			return _emitErr("entry %q has negative offset %d", e.name, e.e.Offset)
		}
		if e.e.Size < 0 {
			return _emitErr("entry %q has negative size %d", e.name, e.e.Size)
		}
		total += uvarintLen(uint64(len(e.name))) + len(e.name) +
			uvarintLen(uint64(e.e.Offset)) + uvarintLen(uint64(e.e.Size))
	}
	// entry offsets are uint32; see the ShardIndex comment
	if total > math.MaxUint32 {
		return _emitErr("packed index too large (%d bytes)", total)
	}

	mm := memsys.PageMM()

	buf, bufSlab, bufPooled := allocBytes(total, mm)
	buf = buf[:shardIdxPrefLen]
	buf = binary.AppendUvarint(buf, uint64(len(cksumTy)))
	buf = append(buf, cksumTy...)
	buf = binary.AppendUvarint(buf, uint64(len(cksumVal)))
	buf = append(buf, cksumVal...)
	buf = binary.AppendUvarint(buf, uint64(idx.srcSize))
	buf = binary.AppendUvarint(buf, uint64(n))

	offs, offsSlab, offsPooled := allocOffsets(n, mm)
	for i := range ents {
		e := &ents[i]
		offs[i] = uint32(len(buf))
		buf = binary.AppendUvarint(buf, uint64(len(e.name)))
		buf = append(buf, e.name...)
		buf = binary.AppendUvarint(buf, uint64(e.e.Offset))
		buf = binary.AppendUvarint(buf, uint64(e.e.Size))
	}
	debug.Func(func() { debug.Assert(len(buf) == total, len(buf), " vs ", total) })

	buf[0] = shardIdxMetaver
	buf[1] = shardIdxFmtTAR
	buf[2] = shardIdxCksumXXH
	h := shardIdxHash(buf)
	binary.BigEndian.PutUint64(buf[3:], h)

	idx.buf, idx.offs = buf, offs
	idx.bufSlab, idx.offsSlab = bufSlab, offsSlab
	idx.bufPooled, idx.offsPooled = bufPooled, offsPooled
	return nil
}

// Pack returns the read-only packed representation, valid until Free is called.
func (idx *ShardIndex) Pack() ([]byte, error) {
	if len(idx.buf) < shardIdxPrefLen {
		return nil, _emitErr("index is not built")
	}
	return idx.buf, nil
}

func (idx *ShardIndex) Free() {
	freeBytes(idx.buf, idx.bufSlab, idx.bufPooled)
	freeOffsets(idx.offs, idx.offsSlab, idx.offsPooled)
	idx.srcCksum = cos.Cksum{}
	idx.srcSize = 0
	idx.buf, idx.offs = nil, nil
	idx.bufSlab, idx.offsSlab = nil, nil
	idx.bufPooled, idx.offsPooled = nil, nil
}

func (idx *ShardIndex) String() string {
	if idx == nil {
		return "shard-index <nil>"
	}
	var metaver byte
	if len(idx.buf) > 0 {
		metaver = idx.buf[0]
	}
	return fmt.Sprintf("shard-index[v%d, entries=%d, packed=%s, source=%s, %s]",
		metaver, len(idx.offs), cos.IEC(len(idx.buf), 1), cos.IEC(idx.srcSize, 1), &idx.srcCksum)
}

func (idx *ShardIndex) Len() int { return len(idx.offs) }

// in-memory footprint of the packed buffer and offsets
func (idx *ShardIndex) MemSize() int64 {
	return int64(cap(idx.buf) + cap(idx.offs)*cos.SizeofI32)
}

// binary search directly over the packed payload; ordering is guaranteed by metaver 2
func (idx *ShardIndex) Lookup(name string) (ShardIndexEntry, bool) {
	lo, hi := 0, len(idx.offs)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		b := idx.buf[idx.offs[mid]:]

		// single-byte length covers every name shorter than 128 bytes
		beg, nlen := 1, int(b[0])
		if b[0] >= 0x80 {
			v, n := binary.Uvarint(b)
			beg, nlen = n, int(v)
		}
		end := beg + nlen

		switch cmp := strings.Compare(cos.UnsafeS(b[beg:end]), name); {
		case cmp == 0:
			offset, n := binary.Uvarint(b[end:])
			size, _ := binary.Uvarint(b[end+n:])
			return ShardIndexEntry{Offset: int64(offset), Size: int64(size)}, true
		case cmp < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return ShardIndexEntry{}, false
}

// decode the complete entry at the given offset (name included) - used by All
func (idx *ShardIndex) at(off uint32) (string, ShardIndexEntry) {
	nlen, n := binary.Uvarint(idx.buf[off:])
	beg := int(off) + n
	end := beg + int(nlen)
	name := cos.UnsafeS(idx.buf[beg:end])

	offset, n := binary.Uvarint(idx.buf[end:])
	end += n
	size, _ := binary.Uvarint(idx.buf[end:])

	return name, ShardIndexEntry{Offset: int64(offset), Size: int64(size)}
}

/////////////////////
// ShardIndexEntry //
/////////////////////

// byte offset of the file's data within the archive
func (e ShardIndexEntry) DataOffset() int64 { return e.Offset + TarBlockSize }

////////////////
// idxDecoder //
////////////////

// read a length-prefixed (uvarint) uint64
func (d *idxDecoder) readU64(field string) (uint64, error) {
	v, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		return 0, _corruptErr("failed to decode %s", field)
	}
	d.off += n
	return v, nil
}

func (d *idxDecoder) readI64(field string) (int64, error) {
	v, err := d.readU64(field)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, _corruptErr("%s overflows int64", field)
	}
	return int64(v), nil
}

// read a length-prefixed (uvarint) string and copy it out of the immutable payload
func (d *idxDecoder) readStr(field string) (string, error) {
	slen, err := d.readU64(field + " length")
	if err != nil {
		return "", err
	}
	// Compare before converting and adding: d.off + int(slen) can overflow
	// even when slen itself fits in an int.
	if slen > uint64(len(d.b)-d.off) {
		return "", _corruptErr("%s overruns buffer", field)
	}
	end := d.off + int(slen)
	// must copy the strings - become idx.srcCksum, and SrcCksum().Clone()
	s := string(d.b[d.off:end])
	d.off = end
	return s, nil
}

// advance past one entry, validate its framing, and return its name
// (which aliases the packed buffer (see ShardIndex))
func (d *idxDecoder) nextEntry() (string, error) {
	nlen, err := d.readU64("name length")
	if err != nil {
		return "", err
	}
	if nlen > uint64(len(d.b)-d.off) {
		return "", _corruptErr("name overruns buffer")
	}
	beg := d.off
	d.off += int(nlen)
	name := cos.UnsafeS(d.b[beg:d.off])

	if _, err := d.readI64("offset"); err != nil {
		return "", err
	}
	if _, err := d.readI64("size"); err != nil {
		return "", err
	}
	return name, nil
}

// read and unpack an index into owned storage; the caller must call Free;
// a nil memsys instance allocates the index on the heap
func ReadShardIndex(r io.Reader, size int64, mm *memsys.MMSA) (*ShardIndex, error) {
	if size < 0 || size > math.MaxUint32 {
		return nil, _corruptErr("invalid buffer size %d", size)
	}

	buf, slab, pooled := allocBytes(int(size), mm)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		freeBytes(buf, slab, pooled)
		if cos.IsAnyEOF(err) {
			return nil, _corruptErr("truncated index payload: got %d of %d", n, size)
		}
		return nil, err
	}

	idx := &ShardIndex{buf: buf, bufSlab: slab, bufPooled: pooled}
	if err := idx.unpack(buf, mm); err != nil {
		idx.Free()
		return nil, err
	}
	return idx, nil
}

func (idx *ShardIndex) unpack(b []byte, mm *memsys.MMSA) error {
	if len(b) < shardIdxPrefLen {
		return _corruptErr("buffer underrun (%d bytes)", len(b))
	}
	if len(b) > math.MaxUint32 {
		return _corruptErr("buffer too large (%d bytes)", len(b))
	}

	// The checksum type is part of the fixed preamble shared by both versions.
	if b[2] != shardIdxCksumXXH {
		return _corruptErr("unsupported checksum type %d", b[2])
	}

	// incl. [backward compatibility]
	var h uint64
	switch b[0] {
	case shardIdxMetaver:
		h = shardIdxHash(b)
	case shardIdxMetaverV1:
		h = onexxh.Checksum64S(b[shardIdxPrefLen:], cos.MLCG32)
	case 0:
		return _corruptErr("invalid meta-version %d", b[0])
	default:
		return _corruptErr("meta-version %d is newer than supported %d", b[0], shardIdxMetaver)
	}
	storedHash := binary.BigEndian.Uint64(b[3:])
	if h != storedHash {
		return _corruptErr("checksum mismatch (stored %016x, computed %016x)", storedHash, h)
	}
	if b[1] != shardIdxFmtTAR {
		return _corruptErr("unsupported format %d", b[1])
	}

	// TODO -- FIXME: something like XactDemandShardIndex.Do(...)
	if b[0] == shardIdxMetaverV1 {
		return _staleErr("meta-version %d predates %d - rebuild required", b[0], shardIdxMetaver)
	}

	// decode over the whole buffer, so recorded entry offsets are absolute within it
	d := idxDecoder{b: b, off: shardIdxPrefLen}

	cksumTy, err := d.readStr("src cksum type")
	if err != nil {
		return err
	}
	cksumVal, err := d.readStr("src cksum value")
	if err != nil {
		return err
	}
	cksum := cos.NewCksum(cksumTy, cksumVal)
	debug.Func(func() { err := cksum.Validate(); debug.AssertNoErr(err) })

	srcSize, err := d.readI64("src size")
	if err != nil {
		return err
	}
	count, err := d.readU64("entry count")
	if err != nil {
		return err
	}
	if count > shardIdxMaxEntries {
		return _corruptErr("entry count %d exceeds maximum %d", count, shardIdxMaxEntries)
	}

	if count > uint64((len(b)-d.off)/3) {
		return _corruptErr("entry count %d exceeds remaining payload", count)
	}

	// metaver 2 guarantees strict name order
	offs, offsSlab, offsPooled := allocOffsets(int(count), mm)
	idx.offs, idx.offsSlab, idx.offsPooled = offs, offsSlab, offsPooled
	var prev string
	for i := range count {
		offs[i] = uint32(d.off)
		name, err := d.nextEntry()
		if err != nil {
			return err
		}
		if i > 0 && name <= prev {
			return _corruptErr("entries out of order at %d", i)
		}
		prev = name
	}
	if d.off != len(b) {
		return _corruptErr("trailing data (%d bytes)", len(b)-d.off)
	}

	idx.srcCksum = *cksum
	idx.srcSize = srcSize
	idx.buf = b
	return nil
}

func (idx *ShardIndex) SrcSize() int64       { return idx.srcSize }
func (idx *ShardIndex) SrcCksum() *cos.Cksum { return &idx.srcCksum }

// whether the index was built from a different version of the shard
func (idx *ShardIndex) IsStale(cksum *cos.Cksum, size int64) bool {
	if !cos.NoneC(&idx.srcCksum) && !idx.srcCksum.Equal(cksum) {
		return true
	}
	return idx.srcSize != size
}
