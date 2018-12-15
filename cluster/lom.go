// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

//
// Local Object Metadata (LOM) is a locally stored object metadata comprising:
// - version, atime, checksum, size, etc. object attributes and flags
// - user and internally visible object names
// - associated runtime context including properties and configuration of the
//   bucket that contains the object, etc.
//

// actions - lom (LOM) state can be filled by applying those incrementally, on as needed basis
const (
	LomFstat = 1 << iota
	LomVersion
	LomAtime
	LomCksum
	LomCksumMissingRecomp
	LomCksumPresentRecomp
)

type (
	LOM struct {
		// this object's runtime context
		T           Target
		Bucketmd    *BMD
		AtimeRespCh chan *atime.Response
		Cksumcfg    *cmn.CksumConf
		Bprops      *cmn.BucketProps
		// this object's names
		Fqn             string
		Bucket, Objname string
		Uname           string
		Newfqn          string
		ParsedFQN       fs.FQNparsed // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		// this object's props
		Version  string
		Atime    time.Time
		Atimestr string
		Size     int64
		Nhobj    cmn.CksumValue
		// other flags
		Bislocal     bool // the bucket (that contains this object) is local
		Doesnotexist bool // the object does not exists (by fstat)
		Misplaced    bool // the object is misplaced
		Badchecksum  bool // this object has a bad checksum
	}
)

func (lom *LOM) RestoredReceived(props *LOM) {
	if props.Version != "" {
		lom.Version = props.Version
	}
	if !props.Atime.IsZero() {
		lom.Atime = props.Atime
	}
	if props.Atimestr != "" {
		lom.Atimestr = props.Atimestr
	}
	if props.Size != 0 {
		lom.Size = props.Size
	}
	lom.Nhobj = props.Nhobj
	lom.Badchecksum = false
	lom.Doesnotexist = false
}

func (lom *LOM) Exists() bool     { return !lom.Doesnotexist }
func (lom *LOM) LRUenabled() bool { return lom.Bucketmd.LRUenabled(lom.Bucket) }
func (lom *LOM) String() string {
	var (
		a string
		s = fmt.Sprintf("%s/%s", lom.Bucket, lom.Objname)
	)
	if glog.V(4) {
		s += fmt.Sprintf("(%s)", lom.Fqn)
		if lom.Size != 0 {
			s += " size=" + cmn.B2S(lom.Size, 1)
		}
		if lom.Version != "" {
			s += " ver=" + lom.Version
		}
		if lom.Nhobj != nil {
			s += " " + lom.Nhobj.String()
		}
	}
	if lom.Doesnotexist {
		a = cmn.DoesNotExist
	}
	if lom.Misplaced {
		if a != "" {
			a += ", "
		}
		a += "locally misplaced"
	}
	if lom.Badchecksum {
		if a != "" {
			a += ", "
		}
		a += "bad checksum"
	}
	if a != "" {
		s += " [" + a + "]"
	}
	return s
}

// main method
func (lom *LOM) Fill(action int) (errstr string) {
	// init
	if lom.Bucket == "" || lom.Objname == "" || lom.Fqn == "" {
		if errstr = lom.init(); errstr != "" {
			return
		}
	}
	//
	// actions
	//
	if action&LomFstat != 0 {
		finfo, err := os.Stat(lom.Fqn)
		if err != nil {
			switch {
			case os.IsNotExist(err):
				lom.Doesnotexist = true
			default:
				errstr = fmt.Sprintf("Failed to fstat %s, err: %v", lom, err)
				lom.T.FSHC(err, lom.Fqn)
			}
			return
		}
		lom.Size = finfo.Size()
	}
	if action&LomVersion != 0 {
		var version []byte
		if version, errstr = fs.GetXattr(lom.Fqn, cmn.XattrObjVersion); errstr != "" {
			return
		}
		lom.Version = string(version)
	}
	if action&LomAtime != 0 { // FIXME: check
		lom.Atimestr, lom.Atime, _ = lom.T.GetAtimeRunner().FormatAtime(lom.Fqn, lom.AtimeRespCh, lom.LRUenabled())
	}
	if action&LomCksum != 0 {
		cksumAction := action&LomCksumMissingRecomp | action&LomCksumPresentRecomp
		if errstr = lom.checksum(cksumAction); errstr != "" {
			return
		}
	}
	return
}

func (lom *LOM) BadChecksum(hksum cmn.CksumValue) (errstr string) {
	if lom.Nhobj != nil {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != %s)", lom, hksum, lom.Nhobj)
	} else {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != nil)", lom, hksum)
	}
	return
}

// xattrs
func (lom *LOM) Persist() (errstr string) {
	if lom.Nhobj != nil {
		_, hval := lom.Nhobj.Get()
		if errstr = fs.SetXattr(lom.Fqn, cmn.XattrXXHashVal, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if lom.Version != "" {
		errstr = fs.SetXattr(lom.Fqn, cmn.XattrObjVersion, []byte(lom.Version))
	}
	if !lom.Atime.IsZero() && lom.LRUenabled() {
		lom.T.GetAtimeRunner().Touch(lom.Fqn, lom.Atime)
	}
	return
}

// IncObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (local bucket versioning (re)enabled, new file)
// the version is set to "1"
func (lom *LOM) IncObjectVersion() (newVersion string, errstr string) {
	const initialVersion = "1"
	if lom.Doesnotexist {
		newVersion = initialVersion
		return
	}

	var vbytes []byte
	if vbytes, errstr = fs.GetXattr(lom.Fqn, cmn.XattrObjVersion); errstr != "" {
		return
	}
	if currValue, err := strconv.Atoi(string(vbytes)); err != nil {
		newVersion = initialVersion
	} else {
		newVersion = fmt.Sprintf("%d", currValue+1)
	}
	return
}

//
// private methods
//

func (lom *LOM) init() (errstr string) {
	bowner := lom.T.GetBowner()
	// resolve fqn
	if lom.Bucket == "" || lom.Objname == "" {
		cmn.Assert(lom.Fqn != "")
		if errstr = lom.resolveFQN(bowner); errstr != "" {
			return
		}
		lom.Bucket, lom.Objname = lom.ParsedFQN.Bucket, lom.ParsedFQN.Objname
		if lom.Bucket == "" || lom.Objname == "" {
			return
		}
	}
	lom.Uname = Uname(lom.Bucket, lom.Objname)
	// bucketmd, bislocal, bprops
	lom.Bucketmd = bowner.Get()
	lom.Bislocal = lom.Bucketmd.IsLocal(lom.Bucket)
	lom.Bprops, _ = lom.Bucketmd.Get(lom.Bucket, lom.Bislocal)
	// cksumcfg
	lom.Cksumcfg = &cmn.GCO.Get().Cksum
	if lom.Bprops != nil && lom.Bprops.Checksum != cmn.ChecksumInherit {
		lom.Cksumcfg = &lom.Bprops.CksumConf
	}
	if lom.Fqn == "" {
		lom.Fqn, errstr = FQN(fs.ObjectType, lom.Bucket, lom.Objname, lom.Bislocal)
	}
	if lom.ParsedFQN.Bucket == "" || lom.ParsedFQN.Objname == "" {
		errstr = lom.resolveFQN(nil, lom.Bislocal)
	}
	return
}

func (lom *LOM) resolveFQN(bowner Bowner, bislocal ...bool) (errstr string) {
	var (
		err    error
		newfqn string
	)
	if len(bislocal) == 0 {
		lom.ParsedFQN, newfqn, err = ResolveFQN(lom.Fqn, bowner)
	} else {
		lom.ParsedFQN, newfqn, err = ResolveFQN(lom.Fqn, nil, lom.Bislocal)
	}
	if err != nil {
		if _, ok := err.(*ErrFqnMisplaced); ok {
			lom.Misplaced = true
			lom.Newfqn = newfqn
		} else {
			errstr = err.Error()
		}
	}
	return
}

// Returns stored checksum (if present) and computed checksum (if requested)
// MAY compute and store a missing (xxhash) checksum
//
// Checksums: brief theory of operations ========================================
//
// * objects are stored in the cluster with their content checksums and in accordance
//   with their bucket configurations.
// * xxhash is the system-default checksum.
// * user can override the system default on a bucket level, by setting checksum=none.
// * bucket (re)configuration can be done at any time.
// * an object with a bad checksum cannot be retrieved (via GET) and cannot be replicated
//   or migrated.
// * GET and PUT operations support an option to validate checksums.
// * validation is done against a checksum stored with an object (GET), or a checksum
//   provided by a user (PUT).
// * replications and migrations are always protected by checksums.
// * when two objects in the cluster have identical (bucket, object) names and checksums,
//   they are considered to be full replicas of each other.
// ==============================================================================
func (lom *LOM) checksum(action int) (errstr string) {
	var (
		storedCksum, computedCksum string
		algo                       = lom.Cksumcfg.Checksum
	)
	if lom.Cksumcfg.Checksum == cmn.ChecksumNone {
		return
	}
	cmn.Assert(algo == cmn.ChecksumXXHash, fmt.Sprintf("Unsupported checksum algorithm '%s'", algo))

	if lom.Nhobj != nil {
		_, storedCksum = lom.Nhobj.Get()
	} else if storedCksum, errstr = fs.GetXattrCksum(lom.Fqn, algo); errstr != "" {
		lom.T.FSHC(errors.New(errstr), lom.Fqn)
		return
	} else if storedCksum != "" {
		lom.Nhobj = cmn.NewCksum(algo, storedCksum)
	}
	if action == 0 {
		return
	}
	// compute
	if storedCksum == "" && action&LomCksumMissingRecomp != 0 {
		if computedCksum, errstr = lom.recomputeXXHash(lom.Fqn, lom.Size); errstr != "" {
			return
		}
		if errstr = fs.SetXattr(lom.Fqn, cmn.XattrXXHashVal, []byte(computedCksum)); errstr != "" {
			lom.Nhobj = nil
			lom.T.FSHC(errors.New(errstr), lom.Fqn)
			return
		}
		lom.Nhobj = cmn.NewCksum(algo, computedCksum)
		return
	}
	if storedCksum != "" && action&LomCksumPresentRecomp != 0 {
		if computedCksum, errstr = lom.recomputeXXHash(lom.Fqn, lom.Size); errstr != "" {
			return
		}
		v := cmn.NewCksum(algo, computedCksum)
		if !cmn.EqCksum(lom.Nhobj, v) {
			lom.Badchecksum = true
			errstr = lom.BadChecksum(v)
		}
	}
	return
}

// helper: a wrapper on top of cmn.ComputeXXHash
func (lom *LOM) recomputeXXHash(fqn string, size int64) (cksum, errstr string) {
	file, err := os.Open(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to open %s, err: %v", fqn, err)
		return
	}
	buf, slab := lom.T.GetMem2().AllocFromSlab2(size)
	cksum, errstr = cmn.ComputeXXHash(file, buf)
	file.Close()
	slab.Free(buf)
	return
}
