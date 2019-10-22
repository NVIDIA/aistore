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

// delimiters
const (
	copyFQNSepa = "\xa6/\xc5"
	recordSepa  = "\xe3/\xbd"
	lenCopySepa = len(copyFQNSepa)
	lenRecSepa  = len(recordSepa)

	xattrBufSize = 4 * cmn.KiB
)

func (lom *LOM) LoadMetaFromFS() error { _, err := lom.lmfs(true); return err }

func (lom *LOM) lmfs(populate bool) (md *lmeta, err error) {
	slab, err := lom.T.GetMem2().GetSlab2(xattrBufSize)
	cmn.AssertNoErr(err)
	b := slab.Alloc()
	read, err := fs.GetXattrBuf(lom.FQN, cmn.XattrLOM, b)
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
	var (
		slab *memsys.Slab2
		buf  []byte
	)
	slab, err = lom.T.GetMem2().GetSlab2(xattrBufSize)
	cmn.AssertNoErr(err)
	buf = slab.Alloc()
	_, err = lom.persistMd(buf)
	slab.Free(buf)
	return
}

func (lom *LOM) persistMd(buf []byte) (metaCksum uint64, err error) {
	var off int
	metaCksum, off = lom.md.marshal(buf)
	if err = fs.SetXattr(lom.FQN, cmn.XattrLOM, buf[:off]); err != nil {
		lom.T.FSHC(err, lom.FQN)
	}
	return
}

func (lom *LOM) persistMdOnCopies(buf []byte) (copyFQN string, err error) {
	_, off := lom.md.marshal(buf)

	// Try to set the xattr for all the copies
	for copyFQN = range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		if err = fs.SetXattr(copyFQN, cmn.XattrLOM, buf[:off]); err != nil {
			break
		}
	}
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
		return fmt.Errorf("%s (%x != %x)", cmn.BadMetaCksumPrefix, expectedCksum, actualCksum)
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
				mpathInfo, _, err := fs.Mountpaths.FQN2MpathInfo(copyFQN)
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

func _writeCopies(copies fs.MPI, buf []byte, off, ll int) int {
	for copyFQN := range copies {
		off += copy(buf[off:], copyFQN)
		off += copy(buf[off:], copyFQNSepa)
		cmn.Assert(off < ll-1) // bounds check
	}
	if off > 0 {
		off -= lenCopySepa
	}
	return off
}

func (md *lmeta) marshal(buf []byte) (metaCksum uint64, off int) {
	var (
		cksumType, cksumValue string
		b8                    [cmn.SizeofI64]byte
		ll                    = len(buf)
	)
	off = cmn.SizeofI64
	appendMD := func(key int, value string, sepa bool) {
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
		cmn.Assert(off < ll-8) // bounds check
	}
	if md.cksum != nil {
		cksumType, cksumValue = md.cksum.Get()
		appendMD(lomCksumType, cksumType, true)
		appendMD(lomCksumValue, cksumValue, true)
	}
	if md.version != "" {
		appendMD(lomObjVersion, md.version, true)
	}
	binary.BigEndian.PutUint64(b8[0:], uint64(md.size))
	appendMD(lomObjSize, string(b8[0:]), false)
	if len(md.copies) > 0 {
		off += copy(buf[off:], recordSepa)
		appendMD(lomObjCopies, "", false)
		off = _writeCopies(md.copies, buf, off, ll)
	}
	//
	// checksum, prepend, and return
	//
	metaCksum = xxhash.ChecksumString64S(string(buf[cmn.SizeofI64:off]), cmn.MLCG32)
	binary.BigEndian.PutUint64(buf[0:], metaCksum)
	return
}
