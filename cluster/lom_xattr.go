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
	err = md.unmarshal(string(read))
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

func (md *lmeta) unmarshal(mdstr string) (err error) {
	const invalid = "invalid lmeta "
	var (
		payload                           string
		expectedCksum, actualCksum        uint64
		cksumType, cksumValue             string
		haveSize, haveVersion, haveCopies bool
		haveCksumType, haveCksumValue     bool
		last                              bool
	)
	expectedCksum = binary.BigEndian.Uint64([]byte(mdstr))
	payload = mdstr[cmn.SizeofI64:]
	actualCksum = xxhash.ChecksumString64S(payload, cmn.MLCG32)

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
				return errors.New(invalid + "#1")
			}
			cksumValue = val
			haveCksumValue = true
		case lomCksumType:
			if haveCksumType {
				return errors.New(invalid + "#2")
			}
			cksumType = val
			haveCksumType = true
		case lomObjVersion:
			if haveVersion {
				return errors.New(invalid + "#3")
			}
			md.version = val
			haveVersion = true
		case lomObjSize:
			if haveSize {
				return errors.New(invalid + "#4")
			}
			md.size = int64(binary.BigEndian.Uint64([]byte(val)))
			haveSize = true
		case lomObjCopies:
			if haveCopies {
				return errors.New(invalid + "#5")
			}
			copyFQNs := strings.Split(val, copyFQNSepa)
			haveCopies = true
			md.copies = make(fs.MPI, len(copyFQNs))
			for _, copyFQN := range copyFQNs {
				mpathInfo, _, err := fs.Mountpaths.ParseMpathInfo(copyFQN)
				if err != nil {
					glog.Warning(err)
					continue
				}
				md.copies[copyFQN] = mpathInfo
			}
		default:
			return errors.New(invalid + "#6")
		}
	}
	if haveCksumType != haveCksumValue {
		return errors.New(invalid + "#7")
	}
	md.cksum = cmn.NewCksum(cksumType, cksumValue)
	if !haveSize {
		return errors.New(invalid + "#8")
	}
	return
}

func _writeCopies(copies fs.MPI, buf []byte, off int) int {
	for copyFQN := range copies {
		if buf == nil {
			off += len(copyFQN) + len(copyFQNSepa)
			continue
		}
		off += copy(buf[off:], copyFQN)
		off += copy(buf[off:], copyFQNSepa)
	}
	if off > 0 {
		off -= lenCopySepa
	}
	return off
}

func (md *lmeta) marshal(mm *memsys.MMSA) (buf []byte, slab *memsys.Slab, off int) {
	oof := md._marshal(nil)
	buf, slab = mm.Alloc(int64(oof))
	cmn.Assert(len(buf) >= oof)
	off = md._marshal(buf)
	cmn.Assert(off == oof)
	return
}

// buf == nil: dry-run to compute buffer size
func (md *lmeta) _marshal(buf []byte) (off int) {
	var (
		cksumType, cksumValue string
		b8                    [cmn.SizeofI64]byte
	)
	off = cmn.SizeofI64 // keep it for checksum
	marshalMD := func(key int, value string, sepa bool) {
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
	}
	dryrunMD := func(key int, value string, sepa bool) {
		off += cmn.SizeofI16
		off += len(value)
		if sepa {
			off += len(recordSepa)
		}
	}
	// use the one or the other
	f := marshalMD
	if buf == nil {
		f = dryrunMD
	}
	// serialize
	if md.cksum != nil {
		cksumType, cksumValue = md.cksum.Get()
		f(lomCksumType, cksumType, true)
		f(lomCksumValue, cksumValue, true)
	}
	if md.version != "" {
		f(lomObjVersion, md.version, true)
	}
	binary.BigEndian.PutUint64(b8[0:], uint64(md.size))
	f(lomObjSize, string(b8[0:]), false)
	if len(md.copies) > 0 {
		if buf == nil {
			off += len(recordSepa)
		} else {
			off += copy(buf[off:], recordSepa)
		}
		f(lomObjCopies, "", false)
		off = _writeCopies(md.copies, buf, off)
	}
	if buf == nil {
		return // dry run done
	}
	// checksum, prepend, and return
	metaCksum := xxhash.ChecksumString64S(string(buf[cmn.SizeofI64:off]), cmn.MLCG32)
	binary.BigEndian.PutUint64(buf[0:], metaCksum)
	return
}
