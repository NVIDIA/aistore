// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	onexxh "github.com/OneOfOne/xxhash"
)

// LOM
const (
	// backward compat v3.31 and prior
	MetaverLOM_V1 = 1 //nolint:revive // readability

	// current
	MetaverLOM = 2
)

// NOTE: Metadata version 2 (v2)
//
// In addition to persisting LOM flags, v2 also serializes the bucket ID (BID)
// into the `lomBID` field. This is a permanent, persistent association: every
// on-disk object record carries not only its flags but also the full 50+ bits
// of the BID assigned to its bucket at creation time.
//
// Consequences / rationale:
//   * Flags survive eviction/reload cycles, restarts, rebalance, etc.
//
//   * BID provides a durable guard that ties the object to a specific bucket
//     incarnation, protecting against subtle races or leftover files.
//
//   * On load, a mismatch between stored BID and current bucket.BID means the
//     object belongs to a previous generation (e.g., bucket is being destroyed
//     or evicted, mountpath replaced). The v1 code “adopted” the new BID from
//     bucket props (effectively, BMD); v2 loading logic will now fail with
//     cmn.NewErrObjDefunct (thus enforcing strict integrity).
//
//   * Under normal operation (destroy => trash) mismatches must be extremely
//     unlikely.
//
// Effectively, v2 moves from “best-effort flags only” to a strong, persistent
// identity: {flags, BID}. This makes objects self-describing and detectable
// even if cached state is lost.

// TODO -- FIXME: the layout is getting obsolete and must be updated
//
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
// adding more checksums will likely require a new MetaverLOM version
const mdCksumTyXXHash = 1

// on-disk xattr
const (
	xattrLOM = "user.ais.lom"

	// TODO -- FIXME: revisit and consider 1) g.pmm or 2) g.ssm with 8K slab
	xattrLomSize = memsys.MaxSmallSlabSize
)

// cmd/xmeta support
const (
	DumpLomEnvVar = "AIS_DUMP_LOM"
)

const lomDirtyMask = uint64(1 << 63)

const (
	badLmeta = "bad lmeta"
)

// packing format: enum internal attrs
const (
	packedCksumT = iota
	packedCksumV
	packedVer
	packedSize
	packedCopies
	packedCustom
	packedLid
)

// packing format: separators
const (
	stringSepa  = "\x00"
	stringSepaB = '\x00'

	customSepa  = "\x01"
	customSepaB = '\x01'

	recordSepa = "\xe3/\xbd"

	lenStrSepa = 1
	lenRecSepa = len(recordSepa)
)

const prefLen = 10 // 10B prefix [ version = 1 | checksum-type | 64-bit xxhash ]

const getxattr = "getxattr" // syscall

// usage: unit tests only
func (lom *LOM) TestAtime() error {
	_, atimefs, _, err := lom.Fstat(true /*get-atime*/)
	if err != nil {
		return err
	}
	lom.md.Atime = atimefs
	lom.md.atimefs = uint64(atimefs)
	return nil
}

// NOTE usage: tests and `xmeta` only; ignores `dirty`
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

	uname := lom.bck.MakeUname(lom.ObjName)
	lom.md.uname = cos.UnsafeSptr(uname)

	return nil
}

func whingeLmeta(cname string, err error) (*lmeta, error) {
	if cos.IsErrXattrNotFound(err) {
		return nil, cmn.NewErrLmetaNotFound(cname, err)
	}
	return nil, os.NewSyscallError(getxattr, fmt.Errorf("%s, err: %w", cname, err))
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
	b, err = lom.GetXattr(buf)
	if err != nil {
		slab.Free(buf)
		if err != syscall.ERANGE {
			return whingeLmeta(lom.Cname(), err)
		}
		debug.Assert(mdSize < xattrLomSize)
		// 2nd attempt: max-size
		buf, slab = g.smm.AllocSize(xattrLomSize)
		b, err = lom.GetXattr(buf)
		if err != nil {
			slab.Free(buf)
			return whingeLmeta(lom.Cname(), err)
		}
	}
	md, err = lom.unpack(b, mdSize, populate)
	slab.Free(buf)
	return md, err
}

func (lom *LOM) unpack(buf []byte, mdSize int64, populate bool) (md *lmeta, _ error) {
	size := int64(len(buf))
	if size == 0 {
		nlog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		return nil, os.NewSyscallError(getxattr, syscall.ENOENT)
	}
	if size < prefLen {
		return nil, fmt.Errorf("%s: too short (%d)", badLmeta, size)
	}

	switch metaver := buf[0]; metaver {
	case MetaverLOM_V1, MetaverLOM:
		md = &lom.md
		if !populate {
			md = &lmeta{}
		}
		if err := md.unpack(buf); err != nil {
			return nil, cmn.NewErrLmetaCorrupted(err)
		}
		// fixup v1 BID
		// (affects either lom.md or scratch md depending on `populate`)
		if metaver == MetaverLOM_V1 && lom.Bprops() != nil {
			md.lid = md.lid.setbid(lom.Bprops().BID)
		}
	default:
		return nil, fmt.Errorf("%s: unknown LOM meta-version %d", badLmeta, metaver)
	}

	// TODO: remove
	debug.Assertf(md.lid.haslmfl(lmflChunk) == fs.HasXattr(lom.FQN, xattrChunk),
		"lmflChunk %t != %t manifest",
		md.lid.haslmfl(lmflChunk), fs.HasXattr(lom.FQN, xattrChunk))

	_mdsize(size, mdSize)
	return md, nil
}

func (lom *LOM) PersistMain() (err error) {
	atime := lom.AtimeUnix()
	debug.Assert(cos.IsValidAtime(atime))
	if atime < 0 /*prefetch*/ || !lom.WritePolicy().IsImmediate() /*write-never, write-delayed*/ {
		lom.md.makeDirty()
		lom.Recache()
		return nil
	}
	// write-immediate (default)
	buf := lom.pack()
	if err = lom.SetXattr(buf); err != nil {
		lom.UncacheDel()
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	} else {
		lom.md.clearDirty()
		lom.Recache()
	}
	g.smm.Free(buf)
	return err
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
	if err = lom.SetXattr(buf); err != nil {
		lom.UncacheDel()
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
		if err = fs.SetXattr(copyFQN, xattrLOM, buf); err != nil {
			break
		}
	}
	g.smm.Free(buf)
	return
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
	debug.Assert(size <= xattrLomSize)
	_mdsize(size, lmsize)
	return
}

func _mdsize(size, mdSize int64) {
	const grow = memsys.SmallSlabIncStep
	var nsize int64
	if size > mdSize {
		nsize = min(size+grow, xattrLomSize)
		g.maxLmeta.CAS(mdSize, nsize)
	} else if mdSize == xattrLomSize && size < xattrLomSize-grow {
		nsize = min(size+grow, (size+xattrLomSize)/2)
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
		haveLid                           bool
		haveCksumType, haveCksumValue     bool
		last                              bool
	)
	if buf[1] != mdCksumTyXXHash {
		return fmt.Errorf("%s: unknown checksum %d", badLmeta, buf[1])
	}
	payload = buf[prefLen:]
	actualCksum = onexxh.Checksum64S(buf[prefLen:], cos.MLCG32)
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
				key := entries[i]
				custom[key] = entries[i+1]
				if key == cmn.OrigFntl {
					md.lid = md.lid.setlmfl(lmflFntl)
				}
			}
			md.SetCustomMD(custom)
		case packedLid:
			if haveLid {
				return errors.New(badLmeta + " #6")
			}
			md.lid = lomBID(binary.BigEndian.Uint64(record[cos.SizeofI16:]))
			haveLid = true
		default:
			return errors.New(badLmeta + " #101")
		}
	}

	if haveCksumType != haveCksumValue {
		return errors.New(badLmeta + " #102")
	}
	md.Cksum = cos.NewCksum(cksumType, cksumValue)
	if !haveSize {
		return errors.New(badLmeta + " #103")
	}
	return nil
}

func (md *lmeta) pack(mdSize int64) (buf []byte) {
	buf, _ = g.smm.AllocSize(mdSize)
	buf = buf[:prefLen] // hold it for md-xattr checksum (below)

	// checksum
	cksumType, cksumValue := md.Cksum.Get()
	buf = _packRecordS(buf, packedCksumT, cksumType, true)
	buf = _packRecordS(buf, packedCksumV, cksumValue, true)

	// version
	if v := md.Version(); v != "" {
		buf = _packRecordS(buf, packedVer, v, true)
	}

	// size
	var b8 [cos.SizeofI64]byte
	binary.BigEndian.PutUint64(b8[:], uint64(md.Size))
	buf = _packRecordB(buf, packedSize, b8[:], false)

	// lid (v2)
	binary.BigEndian.PutUint64(b8[:], uint64(md.lid))
	buf = g.smm.AppendString(buf, recordSepa)
	buf = _packRecordB(buf, packedLid, b8[:], false)

	// copies
	if len(md.copies) > 0 {
		buf = g.smm.AppendString(buf, recordSepa)
		buf = _packRecordS(buf, packedCopies, "", false)
		buf = _packCopies(buf, md.copies)
	}

	// custom md
	if custom := md.GetCustomMD(); len(custom) > 0 {
		buf = g.smm.AppendString(buf, recordSepa)
		buf = _packRecordS(buf, packedCustom, "", false)
		buf = _packCustom(buf, custom)
	}

	// checksum, prepend, and return
	buf[0] = MetaverLOM
	buf[1] = mdCksumTyXXHash
	mdCksumValue := onexxh.Checksum64S(buf[prefLen:], cos.MLCG32)
	binary.BigEndian.PutUint64(buf[2:], mdCksumValue)
	return buf
}

func _packRecordB(buf []byte, key int, value []byte, sepa bool) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], uint16(key))
	buf = g.smm.AppendBytes(buf, bkey[:])
	buf = g.smm.AppendBytes(buf, value)
	if sepa {
		buf = g.smm.AppendString(buf, recordSepa)
	}
	return buf
}

func _packRecordS(buf []byte, key int, value string, sepa bool) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], uint16(key))
	buf = g.smm.AppendBytes(buf, bkey[:])
	if value != "" {
		buf = g.smm.AppendString(buf, value)
	}
	if sepa {
		buf = g.smm.AppendString(buf, recordSepa)
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
		buf = g.smm.AppendString(buf, copyFQN)
		if i < num {
			buf = g.smm.AppendB(buf, stringSepaB)
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
		buf = g.smm.AppendString(buf, k)
		buf = g.smm.AppendB(buf, customSepaB)
		buf = g.smm.AppendString(buf, v)
		if i < num {
			buf = g.smm.AppendB(buf, customSepaB)
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
