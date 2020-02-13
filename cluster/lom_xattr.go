// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"syscall"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/OneOfOne/xxhash"
)

// The whole layout of the lom xattr looks like this:
//
// | ------------------ PREAMBLE ----------------- | --- MD VALUES ---- |
// | --- 1 --- | ----- 1 ----- | -- [CKSUM LEN] -- | - [METADATA LEN] - |
// |  version  | checksum-type |   checksum-value  | ---- metadata ---- |
//
// version - determines the layout version. Thanks to this we can be backward
//  compatible and deprecate old versions if needed.
// checksum-type - determines the checksum algorithm used to compute checksum
//  of the metadata.
// checksum-value - computed checksum of the metadata. The length of the checksum
//  can vary depending on the checksum algorithm.
// metadata - the rest of the layout. The content of the metadata can vary depending
//  on the version of the layout.

const (
	// Describes the latest version of lom xattr layout used in AIS.
	mdVerLatest = 1
)

const (
	// TODO: This should be merged with `cmn/cksum.go`
	// NOTE: These constants are append only. It is forbidden to remove or change
	//  the order of the `mdCksumTy*`.
	mdCksumTyNone = iota
	mdCksumTyXXHash
	mdCksumTyLast
)

var (
	mdCksumValueLen = [mdCksumTyLast]uint16{
		0,                     // mdCksumTyNone
		uint16(cmn.SizeofI64), // mdCksumTyXXHash
	}
)

// on-disk LOM attribute names (NOTE: changing any of this might break compatibility)
const (
	lomCksumType = iota
	lomCksumValue
	lomObjVersion
	lomObjSize
	lomObjCopies
)

const (
	XattrLOM     = "user.ais.lom"
	xattrMaxSize = memsys.PageSize
)

// packing format separators
const (
	copyFQNSepa = "\x00"
	recordSepa  = "\xe3/\xbd"
	lenCopySepa = len(copyFQNSepa)
	lenRecSepa  = len(recordSepa)
)

func (lom *LOM) LoadMetaFromFS() error { _, err := lom.lmfs(true); return err }

func (lom *LOM) lmfs(populate bool) (md *lmeta, err error) {
	slab, err := lom.T.GetMMSA().GetSlab(xattrMaxSize)
	cmn.AssertNoErr(err)
	b := slab.Alloc()
	read, err := fs.GetXattrBuf(lom.FQN, XattrLOM, b)
	if err != nil {
		slab.Free(b)
		return
	}
	if len(read) == 0 {
		glog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		err = syscall.ENOENT
		slab.Free(b)
		return
	}
	md = &lom.md
	if !populate {
		md = &lmeta{}
	}
	err = md.unmarshal(read)
	slab.Free(b)
	return
}

// TODO: in case of error previous metadata should be restored.
func (lom *LOM) Persist() (err error) {
	buf, slab, off := lom.md.marshal(lom.T.GetSmallMMSA())
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf[:off]); err != nil {
		lom.T.FSHC(err, lom.FQN)
	}
	slab.Free(buf)
	return
}

func (lom *LOM) persistMdOnCopies() (copyFQN string, err error) {
	buf, slab, off := lom.md.marshal(lom.T.GetSmallMMSA())
	// replicate for all the copies
	for copyFQN = range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		if err = fs.SetXattr(copyFQN, XattrLOM, buf[:off]); err != nil {
			break
		}
	}
	slab.Free(buf)
	return
}

//
// lmeta
//

func (md *lmeta) unmarshal(mdBuf []byte) (err error) {
	const invalid = "invalid lmeta"
	var (
		payload                           string
		expectedCksum, actualCksum        uint64
		cksumType, cksumValue             string
		haveSize, haveVersion, haveCopies bool
		haveCksumType, haveCksumValue     bool
		last                              bool
	)

	if len(mdBuf) < 2 {
		return fmt.Errorf(
			"%s: metadata too short (got: %d, expected at least: 2)",
			invalid, len(mdBuf),
		)
	}

	version := mdBuf[0]
	if version > mdVerLatest || version == 0 {
		return fmt.Errorf(
			"%s: version of metadata is invalid (got: %d, expected: (0, %d])",
			invalid, version, mdVerLatest,
		)
	}

	mdCksumTy := mdBuf[1]
	if mdCksumTy >= mdCksumTyLast || mdCksumTy <= mdCksumTyNone {
		return fmt.Errorf(
			"%s: metadata checksum type is invalid (got: %d, expected: (%d, %d))",
			invalid, mdCksumTy, mdCksumTyNone, mdCksumTyLast,
		)
	}

	preambleLen := 1 + 1 + mdCksumValueLen[mdCksumTy]
	if len(mdBuf) < int(preambleLen) {
		return fmt.Errorf(
			"%s: metadata too short (got: %d, expected at least: %d)",
			invalid, len(mdBuf), preambleLen,
		)
	}

	switch mdCksumTy {
	case mdCksumTyXXHash:
		payload = string(mdBuf[preambleLen:])
		actualCksum = xxhash.Checksum64S(mdBuf[preambleLen:], cmn.MLCG32)
		expectedCksum = binary.BigEndian.Uint64(mdBuf[2:])
	default:
		cmn.AssertMsg(false, "not yet implemented")
	}
	if expectedCksum != actualCksum {
		s := fmt.Sprintf("%v", md)
		return cmn.NewBadMetaCksumError(expectedCksum, actualCksum, s)
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
		val := record[cmn.SizeofI16:]
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
			md.version = val
			haveVersion = true
		case lomObjSize:
			if haveSize {
				return errors.New(invalid + " #4")
			}
			md.size = int64(binary.BigEndian.Uint64([]byte(val)))
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

				mpathInfo, _, err := fs.Mountpaths.ParseMpathInfo(copyFQN)
				if err != nil {
					// Mountpath with the copy is missing.
					if glog.V(4) {
						glog.Warning(err)
					}
					continue
				}
				md.copies[copyFQN] = mpathInfo
			}
		default:
			return errors.New(invalid + " #6")
		}
	}
	if haveCksumType != haveCksumValue {
		return errors.New(invalid + " #7")
	}
	md.cksum = cmn.NewCksum(cksumType, cksumValue)
	if !haveSize {
		return errors.New(invalid + " #8")
	}
	return
}

func (md *lmeta) marshal(mm *memsys.MMSA) (buf []byte, slab *memsys.Slab, off int) {
	oof := md._marshal(nil, mdCksumTyXXHash)
	buf, slab = mm.Alloc(int64(oof))
	cmn.Assert(len(buf) >= oof)
	off = md._marshal(buf, mdCksumTyXXHash)
	cmn.Assert(off == oof)
	return
}

func _writeCopies(buf []byte, off int, copies fs.MPI) int {
	var (
		i   int
		num = len(copies)
	)
	for copyFQN := range copies {
		cmn.Assert(copyFQN != "")

		i++
		if buf == nil {
			off += len(copyFQN)
			if i < num {
				off += lenCopySepa
			}
			continue
		}
		off += copy(buf[off:], copyFQN)
		if i < num {
			off += copy(buf[off:], copyFQNSepa)
		}
	}
	return off
}

func _marshalMD(buf []byte, off, key int, value string, sepa bool) int {
	var (
		bkey [cmn.SizeofI16]byte
		bb   = bkey[0:]
	)
	binary.BigEndian.PutUint16(bb, uint16(key))
	off += copy(buf[off:], bb)
	off += copy(buf[off:], value)
	if sepa {
		off += copy(buf[off:], recordSepa)
	}
	return off
}

func _dryRunMD(_ []byte, off, _ int, value string, sepa bool) int {
	off += cmn.SizeofI16
	off += len(value)
	if sepa {
		off += lenRecSepa
	}
	return off
}

// buf == nil: dry-run to compute buffer size
func (md *lmeta) _marshal(mdBuf []byte, mdCksumType uint8) (off int) {
	var (
		cksumType, cksumValue string
		b8                    [cmn.SizeofI64]byte

		dryRun      = mdBuf == nil
		preambleLen = 1 + 1 + mdCksumValueLen[mdCksumType]
	)
	off = int(preambleLen) // Keep it for metadata checksum
	// use the one or the other
	f := _marshalMD
	if dryRun {
		f = _dryRunMD
	}
	// serialize
	if md.cksum != nil {
		cksumType, cksumValue = md.cksum.Get()
		off = f(mdBuf, off, lomCksumType, cksumType, true)
		off = f(mdBuf, off, lomCksumValue, cksumValue, true)
	}
	if md.version != "" {
		off = f(mdBuf, off, lomObjVersion, md.version, true)
	}
	binary.BigEndian.PutUint64(b8[0:], uint64(md.size))
	off = f(mdBuf, off, lomObjSize, string(b8[0:]), false)
	if len(md.copies) > 0 {
		if dryRun {
			off += lenRecSepa
		} else {
			off += copy(mdBuf[off:], recordSepa)
		}
		off = f(mdBuf, off, lomObjCopies, "", false)
		off = _writeCopies(mdBuf, off, md.copies)
	}
	if dryRun {
		return // dry run done
	}

	// Checksum the metadata, prepend, and return.
	mdBuf[0] = mdVerLatest
	mdBuf[1] = mdCksumType
	switch mdCksumType {
	case mdCksumTyXXHash:
		mdCksumValue := xxhash.Checksum64S(mdBuf[preambleLen:off], cmn.MLCG32)
		binary.BigEndian.PutUint64(mdBuf[2:], mdCksumValue)
	default:
		cmn.AssertMsg(false, "not yet implemented")
	}
	return
}
