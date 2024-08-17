// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/OneOfOne/xxhash"
)

// On-disk metadata layout - changing any of this must be done with respect
// to backward compatibility (and with caution).
//
// | ------------------ PREAMBLE ----------------- | --- MD VALUES ---- |
// | --- 1 --- | ----- 1 ----- | -- [CKSUM LEN] -- | - [METADATA LEN] - |
// |  version  | checksum-type |   checksum-value  | ---- metadata ---- |
//
// * version - determines the layout version. Thanks to this we can be backward
//   compatible and deprecate old versions if needed.
// * checksum-type - determines the checksum algorithm used to compute checksum
//   of the metadata.
// * checksum-value - computed checksum of the metadata. The length of the checksum
//   can vary depending on the checksum algorithm.
// * metadata - the rest of the layout. The content of the metadata can vary depending
//   on the version of the layout.

// the one and only currently supported checksum type == xxhash;
// adding more checksums will likely require a new cmn.MetaverLOM version
const mdCksumTyXXHash = 1

// on-disk xattr names
const (
	XattrLOM   = "user.ais.lom"
	xattrChunk = "user.ais.chunk"
)

const (
	xattrMaxSize = memsys.MaxSmallSlabSize // lom and chunk, both
)

// cmd/xmeta support
const (
	DumpLomEnvVar = "AIS_DUMP_LOM"
)

const lomDirtyMask = uint64(1 << 63)

const (
	badLmeta = "bad lmeta"
	badChunk = "bad lchunk"
)

// packing format: enum internal attrs
const (
	packedCksumT = iota
	packedCksumV
	packedVer
	packedSize
	packedCopies
	packedCustom
	packedNum
	packedChunk
)

// packing format: separators
const (
	stringSepa = "\x00"
	customSepa = "\x01"
	recordSepa = "\xe3/\xbd"

	lenStrSepa = len(stringSepa)
	lenRecSepa = len(recordSepa)
)

const prefLen = 10 // 10B prefix [ version = 1 | checksum-type | 64-bit xxhash ]

const getxattr = "getxattr" // syscall

// used in tests
func (lom *LOM) AcquireAtimefs() error {
	_, atimefs, _, err := lom.Fstat(true /*get-atime*/)
	if err != nil {
		return err
	}
	lom.md.Atime = atimefs
	lom.md.atimefs = uint64(atimefs)
	return nil
}

// NOTE: used in tests, ignores `dirty`
func (lom *LOM) LoadMetaFromFS() error {
	_, atimefs, _, err := lom.Fstat(true /*get-atime*/)
	if err != nil {
		return err
	}
	if _, err := lom.lmfs(true); err != nil {
		return err
	}
	lom.md.Atime = atimefs
	lom.md.atimefs = uint64(atimefs)
	return nil
}

func whingeLmeta(err error) (*lmeta, error) {
	if cos.IsErrXattrNotFound(err) {
		return nil, cmn.NewErrLmetaNotFound(err)
	}
	return nil, os.NewSyscallError(getxattr, err)
}

func (lom *LOM) lmfsReload(populate bool) (md *lmeta, err error) {
	saved := lom.md.pushrt()
	md, err = lom.lmfs(populate)
	if err == nil {
		md.poprt(saved)
	}
	return
}

func (lom *LOM) lmfs(populate bool) (md *lmeta, err error) {
	var (
		b         []byte
		mdSize    = g.maxLmeta.Load()
		buf, slab = g.smm.AllocSize(mdSize)
	)
	b, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
	if err != nil {
		slab.Free(buf)
		if err != syscall.ERANGE {
			return whingeLmeta(err)
		}
		debug.Assert(mdSize < xattrMaxSize)
		// 2nd attempt: max-size
		buf, slab = g.smm.AllocSize(xattrMaxSize)
		b, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
		if err != nil {
			slab.Free(buf)
			return whingeLmeta(err)
		}
	}
	md, err = lom.unpack(b, mdSize, populate)
	slab.Free(buf)
	return md, err
}

func (lom *LOM) unpack(b []byte, mdSize int64, populate bool) (md *lmeta, err error) {
	size := int64(len(b))
	if size == 0 {
		nlog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		return nil, os.NewSyscallError(getxattr, syscall.ENOENT)
	}
	md = &lom.md
	if !populate {
		md = &lmeta{}
	}
	err = md.unpack(b)
	if err == nil {
		_mdsize(size, mdSize)
	} else {
		err = cmn.NewErrLmetaCorrupted(err)
	}
	return md, err
}

func (lom *LOM) PersistMain() (err error) {
	atime := lom.AtimeUnix()
	debug.Assert(cos.IsValidAtime(atime))
	if atime < 0 /*prefetch*/ || !lom.WritePolicy().IsImmediate() /*write-never, write-delayed*/ {
		lom.md.makeDirty()
		lom.Recache()
		return
	}
	// write-immediate (default)
	buf := lom.pack()
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		lom.Uncache()
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	} else {
		lom.md.clearDirty()
		lom.Recache()
	}
	g.smm.Free(buf)
	return
}

// (caller must set atime; compare with the above)
func (lom *LOM) Persist() (err error) {
	atime := lom.AtimeUnix()
	debug.Assert(cos.IsValidAtime(atime), atime)

	if atime < 0 || !lom.WritePolicy().IsImmediate() {
		lom.md.makeDirty()
		if lom.Bprops() != nil {
			if !lom.IsCopy() {
				lom.Recache()
			}
			lom.setbid(lom.Bprops().BID)
		}
		return
	}

	buf := lom.pack()
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		lom.Uncache()
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	} else {
		lom.md.clearDirty()
		if lom.Bprops() != nil {
			if !lom.IsCopy() {
				lom.Recache()
			}
			lom.setbid(lom.Bprops().BID)
		}
	}
	g.smm.Free(buf)
	return
}

func (lom *LOM) persistMdOnCopies() (copyFQN string, err error) {
	buf := lom.pack()
	// replicate across copies
	for copyFQN = range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		if err = fs.SetXattr(copyFQN, XattrLOM, buf); err != nil {
			break
		}
	}
	g.smm.Free(buf)
	return
}

// NOTE: not clearing dirty flag as the caller will uncache anyway
func (lom *LOM) flushCold(md *lmeta, atime time.Time) {
	if err := lom.flushAtime(atime); err != nil {
		return
	}
	if !md.isDirty() || lom.WritePolicy() == apc.WriteNever {
		return
	}
	lom.md = *md
	if err := lom.syncMetaWithCopies(); err != nil {
		return
	}
	buf := lom.pack()
	if err := fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	}
	g.smm.Free(buf)
}

func (lom *LOM) flushAtime(atime time.Time) error {
	_, _, mtime, err := lom.Fstat(false /*get-atime*/)
	if err != nil {
		return err
	}
	return os.Chtimes(lom.FQN, atime, mtime)
}

func (lom *LOM) pack() (buf []byte) {
	lmsize := g.maxLmeta.Load()
	buf = lom.md.pack(lmsize)
	size := int64(len(buf))
	debug.Assert(size <= xattrMaxSize)
	_mdsize(size, lmsize)
	return
}

func _mdsize(size, mdSize int64) {
	const grow = memsys.SmallSlabIncStep
	var nsize int64
	if size > mdSize {
		nsize = min(size+grow, xattrMaxSize)
		g.maxLmeta.CAS(mdSize, nsize)
	} else if mdSize == xattrMaxSize && size < xattrMaxSize-grow {
		nsize = min(size+grow, (size+xattrMaxSize)/2)
		g.maxLmeta.CAS(mdSize, nsize)
	}
}

///////////
// lmeta //
///////////

func (md *lmeta) makeDirty()    { md.atimefs |= lomDirtyMask }
func (md *lmeta) clearDirty()   { md.atimefs &= ^lomDirtyMask }
func (md *lmeta) isDirty() bool { return md.atimefs&lomDirtyMask == lomDirtyMask }

func (md *lmeta) pushrt() []uint64 {
	return []uint64{uint64(md.Atime), md.atimefs, uint64(md.lid)}
}

func (md *lmeta) poprt(saved []uint64) {
	md.Atime, md.atimefs, md.lid = int64(saved[0]), saved[1], lomBID(saved[2])
}

func (md *lmeta) unpack(buf []byte) error {
	var (
		payload                           []byte
		expectedCksum, actualCksum        uint64
		cksumType, cksumValue             string
		haveSize, haveVersion, haveCopies bool
		haveCksumType, haveCksumValue     bool
		last                              bool
	)
	if len(buf) < prefLen {
		return fmt.Errorf("%s: too short (%d)", badLmeta, len(buf))
	}
	if buf[0] != cmn.MetaverLOM {
		return fmt.Errorf("%s: unknown version %d", badLmeta, buf[0])
	}
	if buf[1] != mdCksumTyXXHash {
		return fmt.Errorf("%s: unknown checksum %d", badLmeta, buf[1])
	}
	payload = buf[prefLen:]
	actualCksum = xxhash.Checksum64S(buf[prefLen:], cos.MLCG32)
	expectedCksum = binary.BigEndian.Uint64(buf[2:])
	if expectedCksum != actualCksum {
		return cos.NewErrMetaCksum(expectedCksum, actualCksum, md.String())
	}

	for off := 0; !last; {
		var (
			record []byte
			i      = bytes.Index(payload[off:], recdupSepa[:])
		)
		if i < 0 {
			record = payload[off:]
			last = true
		} else {
			record = payload[off : off+i]
		}
		key := int(binary.BigEndian.Uint16(record)) // the corresponding 'val' is at rec[cos.SizeofI16:]
		off += i + lenRecSepa
		switch key {
		case packedCksumV:
			if haveCksumValue {
				return errors.New(badLmeta + " #1")
			}
			cksumValue = string(record[cos.SizeofI16:])
			haveCksumValue = true
		case packedCksumT:
			if haveCksumType {
				return errors.New(badLmeta + " #2")
			}
			cksumType = string(record[cos.SizeofI16:])
			haveCksumType = true
		case packedVer:
			if haveVersion {
				return errors.New(badLmeta + " #3")
			}
			md.SetVersion(string(record[cos.SizeofI16:]))
			haveVersion = true
		case packedSize:
			if haveSize {
				return errors.New(badLmeta + " #4")
			}
			md.Size = int64(binary.BigEndian.Uint64(record[cos.SizeofI16:]))
			haveSize = true
		case packedCopies:
			if haveCopies {
				return errors.New(badLmeta + " #5")
			}
			val := string(record[cos.SizeofI16:])
			copyFQNs := strings.Split(val, stringSepa)
			haveCopies = true
			md.copies = make(fs.MPI, len(copyFQNs))
			for _, copyFQN := range copyFQNs {
				if copyFQN == "" {
					return errors.New(badLmeta + " #5.1")
				}

				mpathInfo, _, err := fs.FQN2Mpath(copyFQN)
				if err != nil {
					// Mountpath with the copy is missing.
					if cmn.Rom.FastV(4, cos.SmoduleCore) {
						nlog.Warningln(err)
					}
					// For utilities and tests: fill the map with mpath names always
					if os.Getenv(DumpLomEnvVar) != "" {
						md.copies[copyFQN] = nil
					}
					continue
				}
				md.copies[copyFQN] = mpathInfo
			}
		case packedCustom:
			val := string(record[cos.SizeofI16:])
			entries := strings.Split(val, customSepa)
			custom := make(cos.StrKVs, len(entries)/2)
			for i := 0; i < len(entries); i += 2 {
				custom[entries[i]] = entries[i+1]
			}
			md.SetCustomMD(custom)
		default:
			return errors.New(badLmeta + " #6")
		}
	}
	if haveCksumType != haveCksumValue {
		return errors.New(badLmeta + " #7")
	}
	md.Cksum = cos.NewCksum(cksumType, cksumValue)
	if !haveSize {
		return errors.New(badLmeta + " #8")
	}
	return nil
}

func (md *lmeta) pack(mdSize int64) (buf []byte) {
	buf, _ = g.smm.AllocSize(mdSize)
	buf = buf[:prefLen] // hold it for md-xattr checksum (below)

	// checksum
	cksumType, cksumValue := md.Cksum.Get()
	buf = _packRecord(buf, packedCksumT, cksumType, true)
	buf = _packRecord(buf, packedCksumV, cksumValue, true)

	// version
	if v := md.Version(); v != "" {
		buf = _packRecord(buf, packedVer, v, true)
	}

	// size
	var b8 [cos.SizeofI64]byte
	binary.BigEndian.PutUint64(b8[:], uint64(md.Size))
	buf = _packRecord(buf, packedSize, cos.UnsafeS(b8[:]), false)

	// copies
	if len(md.copies) > 0 {
		buf = g.smm.Append(buf, recordSepa)
		buf = _packRecord(buf, packedCopies, "", false)
		buf = _packCopies(buf, md.copies)
	}

	// custom md
	if custom := md.GetCustomMD(); len(custom) > 0 {
		buf = g.smm.Append(buf, recordSepa)
		buf = _packRecord(buf, packedCustom, "", false)
		buf = _packCustom(buf, custom)
	}

	// checksum, prepend, and return
	buf[0] = cmn.MetaverLOM
	buf[1] = mdCksumTyXXHash
	mdCksumValue := xxhash.Checksum64S(buf[prefLen:], cos.MLCG32)
	binary.BigEndian.PutUint64(buf[2:], mdCksumValue)
	return
}

func _packRecord(buf []byte, key int, value string, sepa bool) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], uint16(key))
	buf = g.smm.Append(buf, cos.UnsafeS(bkey[:]))
	buf = g.smm.Append(buf, value)
	if sepa {
		buf = g.smm.Append(buf, recordSepa)
	}
	return buf
}

func _packCopies(buf []byte, copies fs.MPI) []byte {
	var (
		i   int
		num = len(copies)
	)
	for copyFQN := range copies {
		debug.Assert(copyFQN != "")
		i++
		buf = g.smm.Append(buf, copyFQN)
		if i < num {
			buf = g.smm.Append(buf, stringSepa)
		}
	}
	return buf
}

func _packCustom(buf []byte, md cos.StrKVs) []byte {
	var (
		i   int
		num = len(md)
	)
	for k, v := range md {
		debug.Assert(k != "")
		i++
		buf = g.smm.Append(buf, k)
		buf = g.smm.Append(buf, customSepa)
		buf = g.smm.Append(buf, v)
		if i < num {
			buf = g.smm.Append(buf, customSepa)
		}
	}
	return buf
}

// copy atime _iff_ valid and more recent
func (md *lmeta) cpAtime(from *lmeta) {
	if !cos.IsValidAtime(from.Atime) {
		return
	}
	if !cos.IsValidAtime(md.Atime) || (md.Atime > 0 && md.Atime < from.Atime) {
		md.Atime = from.Atime
	}
}
