// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/OneOfOne/xxhash"
)

// on-disk LOM attribute names
// NOTE: changing any of this will break compatibility
const (
	cksumKindXattr = iota
	cksumValXattr
	versionXattr
	copiesXattr
	deletedXattr
	deletedExprXattr // nanoseconds
	//
	// NOTE: new and/or redefined attributes are to be added after this line
)

// delimiters
const (
	cpyfqnSepa = "\xa1\xb2"
	namvalSepa = "\xae\xbd"
	recordSepa = "\xc5\xdf"
)

const (
	emptyDeletedXattr     = "0"
	emptyDeletedExprXattr = "0"
)

// reads LOM's metadata from disk without storing it in LOM object
func readMetaFromFS(fqn string) (*lmeta, error) {
	metaBytes, errstr := fs.GetXattr(fqn, cmn.XattrLOM)
	if errstr != "" {
		return nil, errors.New(errstr)
	}
	if len(metaBytes) == 0 {
		return nil, nil
	}
	return unmarshalMeta(string(metaBytes))
}

// reads LOM's metadata from disk and stores it in LOM object
func (lom *LOM) LoadMetaFromFS() error {
	fsMD, err := readMetaFromFS(lom.FQN)
	if err != nil {
		return err
	}

	if fsMD == nil {
		return nil
	}

	lom.SetVersion(fsMD.version)
	lom.md.copyFQN = fsMD.copyFQN
	if cksumType := lom.CksumConf().Type; cksumType != cmn.ChecksumNone {
		cmn.Assert(cksumType == cmn.ChecksumXXHash)
		lom.SetCksum(fsMD.cksum)
	}

	return nil
}

// converts string to LOM.md object
func unmarshalMeta(stringMd string) (*lmeta, error) {
	r := strings.SplitN(stringMd, recordSepa, 2)
	if len(r) != 2 || r[1] == "" {
		return nil, fmt.Errorf("expected (checksum, payload), got [%s]", stringMd)
	}
	cksmStr, payloadStr := r[0], r[1]
	expectedCksm, err := strconv.ParseUint(cksmStr, 10, 64)
	if err != nil {
		return nil, err
	}
	actualCksm := xxhash.ChecksumString64S(payloadStr, MLCG32)
	if expectedCksm != actualCksm {
		return nil, fmt.Errorf("BAD CHECKSUM: expected %x, got %x", expectedCksm, actualCksm)
	}
	records := strings.Split(payloadStr, recordSepa)
	md := &lmeta{}
	err = md.setMDFromXattr(records)

	return md, err
}

func (md *lmeta) setMDFromXattr(records []string) error {
	var lomCksumKind, lomCksumVal string

	for _, record := range records {
		if record == "" {
			return errors.New("empty xattr record")
		}
		// split record into name and value
		r := strings.SplitN(record, namvalSepa, 2)
		if len(r) != 2 {
			return fmt.Errorf("expected (name, value) got [%s]", record)
		}
		key, val := r[0], r[1]
		keyN, err := strconv.Atoi(key)
		if err != nil {
			return fmt.Errorf("couldn't parse name [%s]", key)
		}
		switch keyN {
		case cksumValXattr:
			lomCksumVal = val
		case cksumKindXattr:
			lomCksumKind = val
		case versionXattr:
			md.version = val
		case copiesXattr:
			if val != "" {
				md.copyFQN = strings.Split(val, cpyfqnSepa)
			}
		case deletedXattr:
			cmn.Assert(val == emptyDeletedXattr)
		case deletedExprXattr:
			cmn.Assert(val == emptyDeletedExprXattr)
		default:
			glog.Errorf("invalid name [%d]", keyN)
		}
	}
	md.cksum = cmn.NewCksum(lomCksumKind, lomCksumVal)
	return nil
}

// stores on disk attributes of LOM.md
func (lom *LOM) Persist() error {
	fsMeta := marshalMeta(&lom.md)

	if errstr := fs.SetXattr(lom.FQN, cmn.XattrLOM, fsMeta); errstr != "" {
		lom.T.FSHC(errors.New(errstr), lom.FQN)
		return errors.New(errstr)
	}

	return nil
}

// converts LOM.md to string representation
// NOTE: does not call json.Marshal function
func marshalMeta(md *lmeta) []byte {
	var cksmKind, cksmVal string

	if md.cksum != nil {
		cksmKind, cksmVal = md.cksum.Get()
	}
	payload := []string{
		xattrLine(cksumKindXattr, cksmKind),
		xattrLine(cksumValXattr, cksmVal),
		xattrLine(versionXattr, md.version),
		xattrLine(copiesXattr, strings.Join(md.copyFQN, cpyfqnSepa)),
		xattrLine(deletedXattr, emptyDeletedXattr),
		xattrLine(deletedExprXattr, emptyDeletedExprXattr),
	}
	payloadStr := strings.Join(payload, recordSepa)
	metaCksm := xxhash.ChecksumString64S(payloadStr, MLCG32)

	return []byte(strings.Join([]string{strconv.FormatUint(metaCksm, 10), payloadStr}, recordSepa))
}

func xattrLine(key int, value string) string { return strconv.Itoa(key) + namvalSepa + value }
