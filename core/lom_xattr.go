// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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

	onexxh "github.com/OneOfOne/xxhash"
)

// LOM
const (
	// backward compat v3.31 and prior
	MetaverLOM_V1 = 1

	// current
	MetaverLOM = 2
)

// Metadata version 2 (v2)
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
//   * On load, a mismatch between stored BID and current bucket's BID means the
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

// On-disk metadata layout - changing any of this must be done with respect
// to backward compatibility (and with caution).
//
// | -------------------- PREAMBLE -------------------- | --- MD VALUES ---- |
// | ------ 1 ----- | ----- 1 ----- | -- [CKSUM LEN] -- | - [METADATA LEN] - |
// |  meta-version  | checksum-type |   checksum-value  | ---- metadata ---- |
//
// * meta-version - determines on-disk structure of the metadata.
// * checksum-type - determines the metadata checksum type
// * checksum-value - respectively, computed checksum
// * metadata - object metadata payload

const mdCksumTyXXHash = 1 // metadata checksum type: xxhash

// on-disk xattr
const (
	xattrLOM = "user.ais.lom"

	// NOTE: 4K limit
	xattrLomSize = memsys.MaxSmallSlabSize
)

// cmd/xmeta support
const (
	DumpLomEnvVar = "AIS_DUMP_LOM"
)

const lomDirtyMask = cos.MSB64

const (
	badLmeta = "bad lmeta"
)

// packing format: enum internal attrs
const (
	packedCksumT = uint16(iota)
	packedCksumV
	packedVer
	packedSize
	packedCopies
	packedCustom
	packedLid
	packedFlags
)

const (
	haveCksumT = 1 << iota
	haveCksumV
	haveVer
	haveSize
	haveCopies
	haveCustom
	haveLid
	haveFlags
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

func _errLmeta(cname string, err error) (e error) {
	switch {
	case os.IsNotExist(err) || errors.Is(err, syscall.ENOENT):
		e = cos.NewErrNotFound(T, cname)
	case cos.IsErrXattrNotFound(err):
		e = cmn.NewErrLmetaNotFound(cname, err)
	default:
		e = os.NewSyscallError(getxattr, fmt.Errorf("%s: %w", cname, err))
	}
	return
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
			return nil, _errLmeta(lom.Cname(), err)
		}
		debug.Assert(mdSize < xattrLomSize)
		// 2nd attempt: max-size
		buf, slab = g.smm.AllocSize(xattrLomSize)
		b, err = lom.GetXattr(buf)
		if err != nil {
			slab.Free(buf)
			return nil, _errLmeta(lom.Cname(), err)
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
		return nil, cmn.NewErrLmetaCorrupted(fmt.Errorf("%s: too short (%d)", badLmeta, size))
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
		return nil, cmn.NewErrLmetaCorrupted(fmt.Errorf("%s: unknown LOM meta-version %d", badLmeta, metaver))
	}

	_mdsize(size, mdSize)
	return md, nil
}

func (lom *LOM) PersistMain(isChunked bool) error {
	debug.Assertf(lom.bid() == lom.Bprops().BID || lom.bid() == 0, "defunct %s: %x vs %x", lom, lom.bid(), lom.Bprops().BID)
	debug.Assertf(lom.IsLocked() == apc.LockWrite, "%s must be wlocked (have %d)", lom.String(), lom.IsLocked())

	// cleanup when transitioning from 'chunked' to 'monolithic'
	if !isChunked && lom.IsChunked(true /*special: skipVC or not exist*/) {
		lom.clrlmfl(lmflChunk)
		u, err := NewUfest("", lom, true /*must-exist*/)
		debug.AssertNoErr(err)
		if err := u.removeCompleted(true /*except first*/); err != nil {
			nlog.Errorln("failed to remove", u._utag(lom.Cname()), "err:", err) // proceeding anyway
		}
		debug.Assert(u.flags&flCompleted != 0, u._utag(lom.Cname()), " not marked 'completed'")
	}

	atime := lom.AtimeUnix()
	debug.Assert(cos.IsValidAtime(atime))
	if atime < 0 /*prefetch*/ || !lom.WritePolicy().IsImmediate() /*write-never, write-delayed*/ {
		lom.md.makeDirty()
		lom.Recache()
		return nil
	}

	// write-immediate (default)
	buf := lom.pack()
	err := lom.SetXattr(buf)
	g.smm.Free(buf)

	if err == nil {
		lom.md.clearDirty()
		lom.Recache()
		return nil
	}

	lom.UncacheDel()
	T.FSHC(err, lom.Mountpath(), lom.FQN)
	return err
}

// (caller must set atime; compare with the above)
func (lom *LOM) Persist() error {
	atime := lom.AtimeUnix()
	bprops := lom.Bprops()
	debug.Assert(cos.IsValidAtime(atime), atime)

	if atime < 0 || !lom.WritePolicy().IsImmediate() {
		lom.md.makeDirty()
		if bprops != nil {
			if !lom.IsCopy() {
				lom.Recache()
			}
			lom.setbid(bprops.BID)
		}
		return nil
	}

	if bprops != nil {
		lom.setbid(bprops.BID)
	}
	buf := lom.pack()
	err := lom.SetXattr(buf)
	g.smm.Free(buf)

	if err == nil {
		lom.md.clearDirty()
		if bprops != nil && !lom.IsCopy() {
			lom.Recache()
		}
		return nil
	}

	lom.UncacheDel()
	T.FSHC(err, lom.Mountpath(), lom.FQN)
	return err
}

func (lom *LOM) persistMdOnCopies() (copyFQN string, err error) {
	debug.Assertf(lom.bid() == lom.Bprops().BID || lom.bid() == 0, "defunct %s: %x vs %x", lom, lom.bid(), lom.Bprops().BID)
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
	return fs.ChtimeOnly(lom.FQN, atime)
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
		payload                    []byte
		expectedCksum, actualCksum uint64
		cksumType, cksumValue      string
		seen                       uint32
		last                       bool
	)
	if len(buf) < prefLen {
		return errors.New(badLmeta + " short preamble")
	}
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
		key := binary.BigEndian.Uint16(record)
		off += i + lenRecSepa
		switch key {
		case packedCksumV:
			if seen&haveCksumV != 0 {
				return errors.New(badLmeta + " #1")
			}
			cksumValue = string(record[cos.SizeofI16:])
			seen |= haveCksumV
		case packedCksumT:
			if seen&haveCksumT != 0 {
				return errors.New(badLmeta + " #2")
			}
			cksumType = string(record[cos.SizeofI16:])
			seen |= haveCksumT
		case packedVer:
			if seen&haveVer != 0 {
				return errors.New(badLmeta + " #3")
			}
			md.SetVersion(string(record[cos.SizeofI16:]))
			seen |= haveVer
		case packedSize:
			if seen&haveSize != 0 {
				return errors.New(badLmeta + " #4")
			}
			md.Size = int64(binary.BigEndian.Uint64(record[cos.SizeofI16:]))
			seen |= haveSize
		case packedCopies:
			if seen&haveCopies != 0 {
				return errors.New(badLmeta + " #5")
			}
			val := string(record[cos.SizeofI16:])
			copyFQNs := strings.Split(val, stringSepa)
			seen |= haveCopies
			md.copies = make(fs.MPI, len(copyFQNs))
			for _, copyFQN := range copyFQNs {
				if copyFQN == "" {
					return errors.New(badLmeta + " #5.1")
				}

				mpathInfo, _, err := fs.FQN2Mpath(copyFQN)
				if err != nil {
					// Mountpath with the copy is missing.
					if cmn.Rom.V(4, cos.ModCore) {
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
			if seen&haveLid != 0 {
				return errors.New(badLmeta + " #6")
			}
			md.lid = lomBID(binary.BigEndian.Uint64(record[cos.SizeofI16:]))
			seen |= haveLid
		case packedFlags:
			if seen&haveFlags != 0 {
				return errors.New(badLmeta + " #7")
			}
			flags := binary.BigEndian.Uint64(record[cos.SizeofI16:])
			debug.Assert(flags&lmflHRW == 0, "unexpected persisted HRW bit")
			md.flags = (md.flags & lmflHRW) | (flags &^ lmflHRW)
			seen |= haveFlags
		default:
			return errors.New(badLmeta + " #101")
		}
	}

	if seen&haveSize != haveSize {
		return errors.New(badLmeta + " #103")
	}
	return md._setCksum(cksumType, cksumValue, seen&haveCksumT != 0, seen&haveCksumV != 0)
}

func (md *lmeta) _setCksum(cksumT, cksumV string, haveT, haveV bool) error {
	if haveV != haveT {
		return fmt.Errorf("%s %s %q %q", badLmeta, "#102:", cksumT, cksumV)
	}

	md.Cksum = cos.NewCksum(cksumT, cksumV)
	return nil
}

func (md *lmeta) pack(mdSize int64) (buf []byte) {
	buf, _ = g.smm.AllocSize(mdSize)
	buf = buf[:prefLen] // hold it for md-xattr checksum (below)

	// checksum
	var (
		cksumType  = cos.ChecksumNone
		cksumValue string
	)
	if md.Cksum != nil { // compare w/ cos.NoneC
		cksumType, cksumValue = md.Cksum.Get()
	}
	buf = _prsp(buf, cksumType, packedCksumT)
	buf = _prsp(buf, cksumValue, packedCksumV)

	// version
	if v := md.Version(); v != "" {
		buf = _prsp(buf, v, packedVer)
	}

	// size
	var b8 [cos.SizeofI64]byte
	binary.BigEndian.PutUint64(b8[:], uint64(md.Size))
	buf = _prbp(buf, b8[:], packedSize)

	// lid (v2)
	binary.BigEndian.PutUint64(b8[:], uint64(md.lid))
	buf = _prbp(buf, b8[:], packedLid)

	// flags (v2)
	flags := md.flags &^ lmflHRW
	binary.BigEndian.PutUint64(b8[:], flags)
	buf = _prb(buf, b8[:], packedFlags)

	// copies
	if len(md.copies) > 0 {
		buf = g.smm.AppendBytes(buf, recdupSepa[:])
		buf = _prso(buf, packedCopies)
		buf = _pcopies(buf, md.copies)
	}

	// custom md
	if custom := md.GetCustomMD(); len(custom) > 0 {
		buf = g.smm.AppendBytes(buf, recdupSepa[:])
		buf = _prso(buf, packedCustom)
		buf = _pcustom(buf, custom)
	}

	// checksum, prepend, and return
	buf[0] = MetaverLOM
	buf[1] = mdCksumTyXXHash
	mdCksumValue := onexxh.Checksum64S(buf[prefLen:], cos.MLCG32)
	binary.BigEndian.PutUint64(buf[2:], mdCksumValue)
	return buf
}

// _prsp: pack record (string value) and append record separator
// _prso: pack record (key only)
// _prbp: pack record (binary value) and append record separator
// _prb : pack record (binary value) without separator

func _prb(buf, value []byte, key uint16) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], key)
	return g.smm.AppendBytes2(buf, bkey[:], value)
}

func _prbp(buf, value []byte, key uint16) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], key)
	buf = g.smm.AppendBytes2(buf, bkey[:], value)
	return g.smm.AppendBytes(buf, recdupSepa[:])
}

func _prso(buf []byte, key uint16) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], key)
	return g.smm.AppendBytes(buf, bkey[:])
}

func _prsp(buf []byte, value string, key uint16) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], key)
	buf = g.smm.AppendBytes(buf, bkey[:])
	if value != "" {
		buf = g.smm.AppendString(buf, value)
	}
	return g.smm.AppendBytes(buf, recdupSepa[:])
}

func _pcopies(buf []byte, copies fs.MPI) []byte {
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

func _pcustom(buf []byte, md cos.StrKVs) []byte {
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
