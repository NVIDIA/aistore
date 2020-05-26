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

const (
	mdVersion       = 1 // the one and only currently supported version
	mdCksumTyXXHash = 1 // the one and only currently supported checksum type == xxhash
)

const XattrLOM = "user.ais.lom" // on-disk xattr name
const xattrMaxSize = memsys.MaxSmallSlabSize

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

// Custom metadata stored under `lomCustomMD` key.
const (
	SourceObjMD       = "source"
	SourceAmazonObjMD = cmn.ProviderAmazon
	SourceGoogleObjMD = cmn.ProviderGoogle
	SourceAzureObjMD  = cmn.ProviderAzure
	SourceWebObjMD    = "web"

	GoogleVersionObjMD = cmn.ProviderGoogle + ".v"
	GoogleCRC32CObjMD  = cmn.ProviderGoogle + "." + cmn.ChecksumCRC32C
	GoogleMD5ObjMD     = cmn.ProviderGoogle + "." + cmn.ChecksumMD5

	AmazonVersionObjMD = cmn.ProviderAmazon + ".v"
	AmazonMD5ObjMD     = cmn.ProviderAmazon + "." + cmn.ChecksumMD5

	AzureVersionObjMD = cmn.ProviderAzure + ".v"
	AzureMD5ObjMD     = cmn.ProviderAzure + "." + cmn.ChecksumMD5
)

func (lom *LOM) LoadMetaFromFS() error { _, err := lom.lmfs(true); return err }

// TODO -- FIXME: xattrMaxSize == MaxSmallSlabSize is the hard limit
//                support runtime switch small => page allocator
func (lom *LOM) lmfs(populate bool) (md *lmeta, err error) {
	var (
		size      int64
		read      []byte
		mdSize    = maxLmeta.Load()
		mm        = lom.T.GetSmallMMSA()
		buf, slab = mm.Alloc(mdSize)
	)
	read, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
	if err != nil {
		slab.Free(buf)
		if err != syscall.ERANGE {
			return
		}
		cmn.Assert(mdSize < xattrMaxSize)
		// 2nd attempt: max-size
		buf, slab = mm.Alloc(xattrMaxSize)
		read, err = fs.GetXattrBuf(lom.FQN, XattrLOM, buf)
		if err != nil {
			slab.Free(buf)
			return
		}
	}
	size = int64(len(read))
	if size == 0 {
		glog.Errorf("%s[%s]: ENOENT", lom, lom.FQN)
		err = syscall.ENOENT
		slab.Free(buf)
		return
	}
	md = &lom.md
	if !populate {
		md = &lmeta{}
	}
	err = md.unmarshal(read)
	if err == nil {
		lom._recomputeMdSize(size, mdSize)
	}
	slab.Free(buf)
	return
}

func (lom *LOM) Persist() (err error) {
	buf, mm := lom._persist()
	if err = fs.SetXattr(lom.FQN, XattrLOM, buf); err != nil {
		lom.T.FSHC(err, lom.FQN)
	}
	mm.Free(buf)
	return
}

// TODO -- FIXME: xattrMaxSize == MaxSmallSlabSize is the hard limit
//                support runtime switch small => page allocator
func (lom *LOM) _persist() (buf []byte, mm *memsys.MMSA) {
	var (
		size   int64
		lmsize = maxLmeta.Load()
	)
	mm = lom.T.GetSmallMMSA()
	buf = lom.md.marshal(mm, lmsize)

	size = int64(len(buf))
	cmn.Assert(size <= xattrMaxSize)
	lom._recomputeMdSize(size, lmsize)
	return
}

func (lom *LOM) _recomputeMdSize(size, mdSize int64) {
	const grow = memsys.SmallSlabIncStep
	var nsize int64
	if size > mdSize {
		nsize = cmn.MinI64(size+grow, xattrMaxSize)
		maxLmeta.CAS(mdSize, nsize)
	} else if mdSize == xattrMaxSize && size < xattrMaxSize-grow {
		nsize = cmn.MinI64(size+grow, (size+xattrMaxSize)/2)
		maxLmeta.CAS(mdSize, nsize)
	}
}

func (lom *LOM) persistMdOnCopies() (copyFQN string, err error) {
	buf, mm := lom._persist()
	// replicate for all the copies
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

//
// lmeta
//

func (md *lmeta) unmarshal(buf []byte) (err error) {
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
	if buf[0] != mdVersion {
		return fmt.Errorf("%s: unknown version %d", invalid, buf[0])
	}
	if buf[1] != mdCksumTyXXHash {
		return fmt.Errorf("%s: unknown checksum %d", invalid, buf[1])
	}
	payload = string(buf[prefLen:])
	actualCksum = xxhash.Checksum64S(buf[prefLen:], cmn.MLCG32)
	expectedCksum = binary.BigEndian.Uint64(buf[2:])
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
		case lomCustomMD:
			entries := strings.Split(val, customMDSepa)
			md.customMD = make(cmn.SimpleKVs, len(entries)/2)
			for i := 0; i < len(entries); i += 2 {
				md.customMD[entries[i]] = entries[i+1]
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

func (md *lmeta) marshal(mm *memsys.MMSA, mdSize int64) (buf []byte) {
	var (
		cksumType  = cmn.ChecksumNone
		cksumValue string
		b8         [cmn.SizeofI64]byte
	)
	buf, _ = mm.Alloc(mdSize)
	buf = buf[:prefLen] // hold it for md-xattr checksum (below)

	// serialize
	if md.cksum != nil {
		cksumType, cksumValue = md.cksum.Get()
	}
	buf = _marshRecord(mm, buf, lomCksumType, cksumType, true)
	buf = _marshRecord(mm, buf, lomCksumValue, cksumValue, true)
	if md.version != "" {
		buf = _marshRecord(mm, buf, lomObjVersion, md.version, true)
	}
	binary.BigEndian.PutUint64(b8[:], uint64(md.size))
	buf = _marshRecord(mm, buf, lomObjSize, string(b8[:]), false)
	if len(md.copies) > 0 {
		buf = mm.Append(buf, recordSepa)
		buf = _marshRecord(mm, buf, lomObjCopies, "", false)
		buf = _marshCopies(mm, buf, md.copies)
	}
	if len(md.customMD) > 0 {
		buf = mm.Append(buf, recordSepa)
		buf = _marshRecord(mm, buf, lomCustomMD, "", false)
		buf = _marshCustomMD(mm, buf, md.customMD)
	}

	// checksum, prepend, and return
	buf[0] = mdVersion
	buf[1] = mdCksumTyXXHash
	mdCksumValue := xxhash.Checksum64S(buf[prefLen:], cmn.MLCG32)
	binary.BigEndian.PutUint64(buf[2:], mdCksumValue)
	return
}

func _marshRecord(mm *memsys.MMSA, buf []byte, key int, value string, sepa bool) []byte {
	var bkey [cmn.SizeofI16]byte
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
		cmn.Assert(copyFQN != "")
		i++
		buf = mm.Append(buf, copyFQN)
		if i < num {
			buf = mm.Append(buf, copyFQNSepa)
		}
	}
	return buf
}

func _marshCustomMD(mm *memsys.MMSA, buf []byte, md cmn.SimpleKVs) []byte {
	var (
		i   int
		num = len(md)
	)
	for k, v := range md {
		cmn.Assert(k != "" && v != "")
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
