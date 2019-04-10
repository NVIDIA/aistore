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

// xattributes
// All of lom's metadata xattributes (currently checksum, version and copies)
// are stored as a single xattribute, so it's one read to fetch them all
//

const (
	// FIXME: this should be deployment/data specific, not project-wide, development specific

	// these keys are tightly bounded with persistent lom's meta, stored on a disk
	// the values should not be changed, unless change in the underlying structure of data
	// is intended and done with being aware of consequences
	cksumKindXattr   = 0
	cksumValXattr    = 1
	versionXattr     = 2
	copiesXattr      = 3
	deletedXattr     = 4
	deletedExprXattr = 5 // in nanoseconds

	// nextAvailable is next available number(key) of new xattribute
	// If one wishes to add new xattribute, its key should be current value of
	// nextAvailable, and then nextAvailable should be incremented
	// nextAvailable = 7
)

const (
	copyNameSepa = "\"\""
	metaSepa     = ";"

	emptyDeletedXattr     = "0"
	emptyDeletedExprXattr = "0"
)

// reads LOM's metadata from disk without storing it in LOM object
func readMetaFromFS(fqn string) (*lmeta, error) {
	metaBytes, errstr := fs.GetXattr(fqn, cmn.XattrMeta)
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
// NOTE: does not call json.Unmarshal function
func unmarshalMeta(stringMd string) (*lmeta, error) {
	r := strings.SplitN(stringMd, "\n", 2)

	if len(r) != 2 {
		return nil, fmt.Errorf("expected meta to be `checksum payload`, got %s", stringMd)
	}

	cksmStr, payloadStr := r[0], r[1]

	if payloadStr == "" {
		return nil, fmt.Errorf("unexpected empty meta payload")
	}

	expectedCksm, err := strconv.ParseUint(cksmStr, 10, 64)
	if err != nil {
		return nil, err
	}
	actualCksm := xxhash.Checksum64S([]byte(payloadStr), 0)

	if expectedCksm != actualCksm {
		return nil, fmt.Errorf("lom meta checksums don't match; expected %d, got %d", expectedCksm, actualCksm)
	}

	lines := strings.Split(payloadStr, "\n")
	md := &lmeta{}
	err = md.setMDFromXattrLines(lines)

	return md, err
}

func (md *lmeta) setMDFromXattrLines(lines []string) error {
	var lomCksumKind, lomCksumVal string

	for _, line := range lines {
		if line == "" {
			return fmt.Errorf("empty xattr entry")
		}

		// split line into 2 strings, if line was abc;def;ghi
		// key is supposed to be 'abc' and value 'def;ghi'
		r := strings.SplitN(line, metaSepa, 2)

		if len(r) != 2 {
			return fmt.Errorf("expected key;value format; got %s", line)
		}

		key, val := r[0], r[1]

		keyN, err := strconv.Atoi(key)
		if err != nil {
			return fmt.Errorf("couldn't parse key %s to a number", key)
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
				md.copyFQN = strings.Split(val, copyNameSepa)
			}
		case deletedXattr:
			cmn.Assert(val == emptyDeletedXattr)
		case deletedExprXattr:
			cmn.Assert(val == emptyDeletedExprXattr)
		default:
			glog.Warningf("unexpected key %d in lom's meta", keyN)
		}
	}
	md.cksum = cmn.NewCksum(lomCksumKind, lomCksumVal)

	return nil
}

// stores on disk attributes of LOM.md
func (lom *LOM) Persist() error {
	fsMeta := marshalMeta(&lom.md)

	if errstr := fs.SetXattr(lom.FQN, cmn.XattrMeta, fsMeta); errstr != "" {
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
		xattrLine(copiesXattr, strings.Join(md.copyFQN, copyNameSepa)),
		xattrLine(deletedXattr, emptyDeletedXattr),
		xattrLine(deletedExprXattr, emptyDeletedExprXattr),
	}
	payloadStr := strings.Join(payload, "\n")

	metaCksm := xxhash.Checksum64S([]byte(payloadStr), 0)
	return []byte(strings.Join([]string{strconv.FormatUint(metaCksm, 10), payloadStr}, "\n"))
}

func xattrLine(key int, value string) string { return strconv.Itoa(key) + metaSepa + value }
