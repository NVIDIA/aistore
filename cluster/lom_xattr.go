// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
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
// NOTE: adding more checksums will likely require a new cmn.MetaverLOM version
const mdCksumTyXXHash = 1

const (
	XattrLOM      = "user.ais.lom" // on-disk xattr name
	xattrMaxSize  = memsys.MaxSmallSlabSize
	DumpLomEnvVar = "AIS_DUMP_LOM"
)

// packing format internal attrs
const (
	lomCksumType = iota
	lomCksumValue
	lomObjVersion
	lomObjSize
	lomObjCopies
	lomCustomMD
)

// packing format separators
const (
	copyFQNSepa  = "\x00"
	customMDSepa = "\x01"
	recordSepa   = "\xe3/\xbd"
	lenRecSepa   = len(recordSepa)
)

const prefLen = 10 // 10B prefix [ version = 1 | checksum-type | 64-bit xxhash ]

const getxattr = "getxattr" // syscall

// used in tests
func (lom *LOM) AcquireAtimefs() error {
	_, atime, err := ios.FinfoAtime(lom.FQN)
	if err != nil {
		return err
	}
	lom.md.Atime = atime
	lom.md.atimefs = uint64(atime)
	return nil
}

// NOTE: used in tests, ignores `dirty`
func (lom *LOM) LoadMetaFromFS() error {
	_, atime, err := ios.FinfoAtime(lom.FQN)
	if err != nil {
		return err
	}
	if _, err := lom.lmfs(true); err != nil {
		return err
	}
	lom.md.Atime = atime
	lom.md.atimefs = uint64(atime)
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
		size      int64
		read      []byte
		mdSize    = maxLmeta.Load()
		mm        = T.ByteMM()
		buf, slab = mm.AllocSize(mdSize)
	)
	read, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
	if err != nil {
		slab.Free(buf)
		if err != syscall.ERANGE {
			return whingeLmeta(err)
		}
		debug.Assert(mdSize < xattrMaxSize)
		// 2nd attempt: max-size
		buf, slab = mm.AllocSize(xattrMaxSize)
		read, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
		if err != nil {
			slab.Free(buf)
			return whingeLmeta(err)
		}
	}
	size = int64(len(read))
	if size == 0 {
		glog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		err = os.NewSyscallError(getxattr, syscall.ENOENT)
		slab.Free(buf)
		return
	}
	md = &lom.md
	if !populate {
		md = &lmeta{}
	}
	err = md.unmarshal(read)
	if err == nil {
		_recomputeMdSize(size, mdSize)
	} else {
		err = cmn.NewErrLmetaCorrupted(err)
	}
	slab.Free(buf)
	return
}

func (lom *LOM) PersistMain() (err error) {
	atime := lom.AtimeUnix()
	debug.Assert(isValidAtime(atime))
	if atime < 0 /*prefetch*/ || !lom.WritePolicy().IsImmediate() /*write-never, write-delayed*/ {
		lom.md.makeDirty()
		lom.Recache()
		return
	}
	// write-immediate (default)
	buf, mm := lom.marshal()
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		lom.Uncache(true /*delDirty*/)
		T.FSHC(err, lom.FQN)
	} else {
		lom.md.clearDirty()
		lom.Recache()
	}
	mm.Free(buf)
	return
}

// (caller must set atime; compare with the above)
func (lom *LOM) Persist() (err error) {
	atime := lom.AtimeUnix()
	debug.Assert(isValidAtime(atime))

	if atime < 0 || !lom.WritePolicy().IsImmediate() {
		lom.md.makeDirty()
		if lom.Bprops() != nil {
			if !lom.IsCopy() {
				lom.Recache()
			}
			lom.md.bckID = lom.Bprops().BID
		}
		return
	}

	buf, mm := lom.marshal()
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		lom.Uncache(true /*delDirty*/)
		T.FSHC(err, lom.FQN)
	} else {
		lom.md.clearDirty()
		if lom.Bprops() != nil {
			if !lom.IsCopy() {
				lom.Recache()
			}
			lom.md.bckID = lom.Bprops().BID
		}
	}
	mm.Free(buf)
	return
}

func (lom *LOM) persistMdOnCopies() (copyFQN string, err error) {
	buf, mm := lom.marshal()
	// replicate across copies
	for copyFQN = range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		if err = fs.SetXattr(copyFQN, XattrLOM, buf); err != nil {
			break
		}
	}
	mm.Free(buf)
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
	buf, mm := lom.marshal()
	if err := fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		T.FSHC(err, lom.FQN)
	}
	mm.Free(buf)
}

func (lom *LOM) flushAtime(atime time.Time) error {
	finfo, err := os.Stat(lom.FQN)
	if err != nil {
		return err
	}
	mtime := finfo.ModTime()
	return os.Chtimes(lom.FQN, atime, mtime)
}

func (lom *LOM) marshal() (buf []byte, mm *memsys.MMSA) {
	lmsize := maxLmeta.Load()
	mm = T.ByteMM()
	buf = lom.md.marshal(mm, lmsize)
	size := int64(len(buf))
	debug.Assert(size <= xattrMaxSize)
	_recomputeMdSize(size, lmsize)
	return
}

func _recomputeMdSize(size, mdSize int64) {
	const grow = memsys.SmallSlabIncStep
	var nsize int64
	if size > mdSize {
		nsize = cos.MinI64(size+grow, xattrMaxSize)
		maxLmeta.CAS(mdSize, nsize)
	} else if mdSize == xattrMaxSize && size < xattrMaxSize-grow {
		nsize = cos.MinI64(size+grow, (size+xattrMaxSize)/2)
		maxLmeta.CAS(mdSize, nsize)
	}
}

///////////
// lmeta //
///////////

func (md *lmeta) makeDirty()    { md.atimefs |= lomDirtyMask }
func (md *lmeta) clearDirty()   { md.atimefs &= ^lomDirtyMask }
func (md *lmeta) isDirty() bool { return md.atimefs&lomDirtyMask == lomDirtyMask }

func (md *lmeta) pushrt() []uint64 {
	return []uint64{uint64(md.Atime), md.atimefs, md.bckID}
}

func (md *lmeta) poprt(saved []uint64) {
	md.Atime, md.atimefs, md.bckID = int64(saved[0]), saved[1], saved[2]
}

func (md *lmeta) unmarshal(buf []byte) error {
	const invalid = "invalid lmeta"
	var (
		payload                           string
		expectedCksum, actualCksum        uint64
		cksumType, cksumValue             string
		haveSize, haveVersion, haveCopies bool
		haveCksumType, haveCksumValue     bool
		last                              bool
	)
	if len(buf) < prefLen {
		return fmt.Errorf("%s: too short (%d)", invalid, len(buf))
	}
	if buf[0] != cmn.MetaverLOM {
		return fmt.Errorf("%s: unknown version %d", invalid, buf[0])
	}
	if buf[1] != mdCksumTyXXHash {
		return fmt.Errorf("%s: unknown checksum %d", invalid, buf[1])
	}
	payload = string(buf[prefLen:])
	actualCksum = xxhash.Checksum64S(buf[prefLen:], cos.MLCG32)
	expectedCksum = binary.BigEndian.Uint64(buf[2:])
	if expectedCksum != actualCksum {
		return cos.NewBadMetaCksumError(expectedCksum, actualCksum, md.String())
	}

	for off := 0; !last; {
		var (
			record string
			i      = strings.Index(payload[off:], recordSepa)
		)
		if i < 0 {
			record = payload[off:]
			last = true
		} else {
			record = payload[off : off+i]
		}
		key := int(binary.BigEndian.Uint16([]byte(record)))
		val := record[cos.SizeofI16:]
		off += i + lenRecSepa
		switch key {
		case lomCksumValue:
			if haveCksumValue {
				return errors.New(invalid + " #1")
			}
			cksumValue = val
			haveCksumValue = true
		case lomCksumType:
			if haveCksumType {
				return errors.New(invalid + " #2")
			}
			cksumType = val
			haveCksumType = true
		case lomObjVersion:
			if haveVersion {
				return errors.New(invalid + " #3")
			}
			md.Ver = val
			haveVersion = true
		case lomObjSize:
			if haveSize {
				return errors.New(invalid + " #4")
			}
			md.Size = int64(binary.BigEndian.Uint64([]byte(val)))
			haveSize = true
		case lomObjCopies:
			if haveCopies {
				return errors.New(invalid + " #5")
			}
			copyFQNs := strings.Split(val, copyFQNSepa)
			haveCopies = true
			md.copies = make(fs.MPI, len(copyFQNs))
			for _, copyFQN := range copyFQNs {
				if copyFQN == "" {
					return errors.New(invalid + " #5.1")
				}

				mpathInfo, _, err := fs.FQN2Mpath(copyFQN)
				if err != nil {
					// Mountpath with the copy is missing.
					if glog.V(4) {
						glog.Warning(err)
					}
					// For utilities and tests: fill the map with mpath names always
					if os.Getenv(DumpLomEnvVar) != "" {
						md.copies[copyFQN] = nil
					}
					continue
				}
				md.copies[copyFQN] = mpathInfo
			}
		case lomCustomMD:
			entries := strings.Split(val, customMDSepa)
			custom := make(cos.StrKVs, len(entries)/2)
			for i := 0; i < len(entries); i += 2 {
				custom[entries[i]] = entries[i+1]
			}
			md.SetCustomMD(custom)
		default:
			return errors.New(invalid + " #6")
		}
	}
	if haveCksumType != haveCksumValue {
		return errors.New(invalid + " #7")
	}
	md.Cksum = cos.NewCksum(cksumType, cksumValue)
	if !haveSize {
		return errors.New(invalid + " #8")
	}
	return nil
}

func (md *lmeta) marshal(mm *memsys.MMSA, mdSize int64) (buf []byte) {
	var (
		b8                    [cos.SizeofI64]byte
		cksumType, cksumValue = md.Cksum.Get()
	)
	buf, _ = mm.AllocSize(mdSize)
	buf = buf[:prefLen] // hold it for md-xattr checksum (below)

	// serialize
	buf = _marshRecord(mm, buf, lomCksumType, cksumType, true)
	buf = _marshRecord(mm, buf, lomCksumValue, cksumValue, true)
	if md.Ver != "" {
		buf = _marshRecord(mm, buf, lomObjVersion, md.Ver, true)
	}
	binary.BigEndian.PutUint64(b8[:], uint64(md.Size))
	buf = _marshRecord(mm, buf, lomObjSize, string(b8[:]), false)
	if len(md.copies) > 0 {
		buf = mm.Append(buf, recordSepa)
		buf = _marshRecord(mm, buf, lomObjCopies, "", false)
		buf = _marshCopies(mm, buf, md.copies)
	}
	if custom := md.GetCustomMD(); len(custom) > 0 {
		buf = mm.Append(buf, recordSepa)
		buf = _marshRecord(mm, buf, lomCustomMD, "", false)
		buf = _marshCustomMD(mm, buf, custom)
	}

	// checksum, prepend, and return
	buf[0] = cmn.MetaverLOM
	buf[1] = mdCksumTyXXHash
	mdCksumValue := xxhash.Checksum64S(buf[prefLen:], cos.MLCG32)
	binary.BigEndian.PutUint64(buf[2:], mdCksumValue)
	return
}

func _marshRecord(mm *memsys.MMSA, buf []byte, key int, value string, sepa bool) []byte {
	var bkey [cos.SizeofI16]byte
	binary.BigEndian.PutUint16(bkey[:], uint16(key))
	buf = mm.Append(buf, string(bkey[:]))
	buf = mm.Append(buf, value)
	if sepa {
		buf = mm.Append(buf, recordSepa)
	}
	return buf
}

func _marshCopies(mm *memsys.MMSA, buf []byte, copies fs.MPI) []byte {
	var (
		i   int
		num = len(copies)
	)
	for copyFQN := range copies {
		debug.Assert(copyFQN != "")
		i++
		buf = mm.Append(buf, copyFQN)
		if i < num {
			buf = mm.Append(buf, copyFQNSepa)
		}
	}
	return buf
}

func _marshCustomMD(mm *memsys.MMSA, buf []byte, md cos.StrKVs) []byte {
	var (
		i   int
		num = len(md)
	)
	for k, v := range md {
		debug.Assert(k != "")
		i++
		buf = mm.Append(buf, k)
		buf = mm.Append(buf, customMDSepa)
		buf = mm.Append(buf, v)
		if i < num {
			buf = mm.Append(buf, customMDSepa)
		}
	}
	return buf
}

func (md *lmeta) cpAtime(from *lmeta) {
	if !isValidAtime(from.Atime) {
		return
	}
	if !isValidAtime(md.Atime) || (md.Atime > 0 && md.Atime < from.Atime) {
		md.Atime = from.Atime
	}
}
