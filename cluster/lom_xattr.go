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
	"github.com/OneOfOne/xxhash"
)

// on-disk LOM attribute names (NOTE: changing any of this might break compatibility)
const (
	lomCsmKnd = iota
	lomCsmVal
	lomObjVer
	lomObjSiz
	lomObjCps
)

// delimiters
const (
	cpyfqnSepa = "\xa6/\xc5"
	recordSepa = "\xe3/\xbd"
	lenCpySepa = len(cpyfqnSepa)
	lenRecSepa = len(recordSepa)

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

func (lom *LOM) Persist() (err error) {
	slab, err := lom.T.GetMem2().GetSlab2(xattrBufSize)
	cmn.AssertNoErr(err)
	buf := slab.Alloc()

	off := lom.md.marshal(buf)
	if err = fs.SetXattr(lom.FQN, cmn.XattrLOM, buf[:off]); err != nil {
		lom.T.FSHC(err, lom.FQN)
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
		payload                                           string
		expectedCksm, actualCksm                          uint64
		lomCksumKind, lomCksumVal                         string
		haveSiz, haveCsmKnd, haveCsmVal, haveVer, haveCps bool
		last                                              bool
	)
	expectedCksm = binary.BigEndian.Uint64([]byte(mdstr))
	payload = mdstr[cmn.SizeofI64:]
	actualCksm = xxhash.ChecksumString64S(payload, MLCG32)

	if expectedCksm != actualCksm {
		return fmt.Errorf("%s (%x != %x)", cmn.BadCksumPrefix, expectedCksm, actualCksm)
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
		case lomCsmVal:
			if haveCsmVal {
				return errors.New(invalid + "#1")
			}
			lomCksumVal = val
			haveCsmVal = true
		case lomCsmKnd:
			if haveCsmKnd {
				return errors.New(invalid + "#2")
			}
			lomCksumKind = val
			haveCsmKnd = true
		case lomObjVer:
			if haveVer {
				return errors.New(invalid + "#3")
			}
			md.version = val
			haveVer = true
		case lomObjSiz:
			if haveSiz {
				return errors.New(invalid + "#4")
			}
			md.size = int64(binary.BigEndian.Uint64([]byte(val)))
			haveSiz = true
		case lomObjCps:
			if haveCps {
				return errors.New(invalid + "#5")
			}
			cpyfqns := strings.Split(val, cpyfqnSepa)
			haveCps = true
			md.copies = make(fs.MPI, len(cpyfqns))
			for _, cpyfqn := range cpyfqns {
				md.copies[cpyfqn], _ = fs.Mountpaths.FQN2MpathInfo(cpyfqn)
			}
		default:
			return errors.New(invalid + "#6")
		}
	}
	if haveCsmKnd != haveCsmVal {
		return errors.New(invalid + "#7")
	}
	md.cksum = cmn.NewCksum(lomCksumKind, lomCksumVal)
	if !haveSiz {
		return errors.New(invalid + "#8")
	}
	return
}

func _writeCopies(copies fs.MPI, buf []byte, off, ll int) int {
	for c := range copies {
		off += copy(buf[off:], c)
		off += copy(buf[off:], cpyfqnSepa)
		cmn.Assert(off < ll-1) // bounds check
	}
	if off > 0 {
		off -= lenCpySepa
	}
	return off
}

func (md *lmeta) marshal(buf []byte) (off int) {
	var (
		cksmKind, cksmVal string
		b8                [cmn.SizeofI64]byte
		ll                = len(buf)
	)
	off = cmn.SizeofI64
	f := func(key int, value string, sepa bool) {
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
		cksmKind, cksmVal = md.cksum.Get()
		f(lomCsmKnd, cksmKind, true)
		f(lomCsmVal, cksmVal, true)
	}
	if md.version != "" {
		f(lomObjVer, md.version, true)
	}
	binary.BigEndian.PutUint64(b8[0:], uint64(md.size))
	f(lomObjSiz, string(b8[0:]), false)
	if len(md.copies) > 0 {
		off += copy(buf[off:], recordSepa)
		f(lomObjCps, "", false)
		off = _writeCopies(md.copies, buf, off, ll)
	}
	//
	// checksum, prepend, and return
	//
	metaCksm := xxhash.ChecksumString64S(string(buf[cmn.SizeofI64:off]), MLCG32)
	binary.BigEndian.PutUint64(buf[0:], metaCksm)
	return
}
