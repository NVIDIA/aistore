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

	// NOTE: must be the last field
	numXattrs
)

// delimiters
const (
	cpyfqnSepa = "\xa6/\xc5"
	recordSepa = "\xe3/\xbd"
)

func (lom *LOM) LoadMetaFromFS(populate bool) (md *lmeta, err error) {
	var b []byte
	b, err = fs.GetXattr(lom.FQN, cmn.XattrLOM)
	if err != nil {
		return
	}
	if len(b) == 0 {
		glog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		err = syscall.ENOENT
		return
	}
	md = &lom.md
	if !populate {
		md = &lmeta{}
	}
	err = md.unmarshal(string(b))
	return
}

func (lom *LOM) Persist() (err error) {
	meta := lom.md.marshal()
	if err = fs.SetXattr(lom.FQN, cmn.XattrLOM, []byte(meta)); err != nil {
		lom.T.FSHC(err, lom.FQN)
	}
	return
}

//
// lmeta
//

func (md *lmeta) unmarshal(mdstr string) (err error) {
	const invalid = "invalid lmeta "
	var (
		records                                           []string
		payload                                           string
		expectedCksm, actualCksm                          uint64
		lomCksumKind, lomCksumVal                         string
		haveSiz, haveCsmKnd, haveCsmVal, haveVer, haveCps bool
	)
	expectedCksm = binary.BigEndian.Uint64([]byte(mdstr))
	payload = mdstr[cmn.SizeofI64:]
	actualCksm = xxhash.ChecksumString64S(payload, MLCG32)
	if expectedCksm != actualCksm {
		return fmt.Errorf("%s (%x != %x)", badCsum, expectedCksm, actualCksm)
	}
	records = strings.Split(payload, recordSepa)
	for _, record := range records {
		key := int(binary.BigEndian.Uint16([]byte(record)))
		val := record[cmn.SizeofI16:]
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
			if val != "" {
				if haveCps {
					return errors.New(invalid + "#5")
				}
				md.copyFQN = strings.Split(val, cpyfqnSepa)
				haveCps = true
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

func (md *lmeta) marshal() (payload string) {
	var (
		cksmKind, cksmVal string
		records           [numXattrs]string
		b8                [cmn.SizeofI64]byte
		bkey              [cmn.SizeofI16]byte
		bb                []byte
	)
	if md.cksum != nil {
		cksmKind, cksmVal = md.cksum.Get()
	}
	bb = b8[0:]
	binary.BigEndian.PutUint64(bb, uint64(md.size))
	records[lomCsmKnd] = xattrRec(lomCsmKnd, cksmKind, bkey)
	records[lomCsmVal] = xattrRec(lomCsmVal, cksmVal, bkey)
	records[lomObjVer] = xattrRec(lomObjVer, md.version, bkey)
	records[lomObjSiz] = xattrRec(lomObjSiz, string(bb), bkey)
	records[lomObjCps] = xattrRec(lomObjCps, strings.Join(md.copyFQN, cpyfqnSepa), bkey)
	payload = strings.Join(records[0:], recordSepa)
	//
	// checksum, append, and return
	//
	metaCksm := xxhash.ChecksumString64S(payload, MLCG32)
	bb = b8[0:]
	binary.BigEndian.PutUint64(bb, metaCksm)
	return string(bb) + payload
}

func xattrRec(key int, value string, b [cmn.SizeofI16]byte) string {
	bb := b[0:]
	binary.BigEndian.PutUint16(bb, uint16(key))
	return string(bb) + value
}
