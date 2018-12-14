// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

// actions - xmd (objectXprops) state can be filled by applying those incrementally, on as needed basis
const (
	xmdFstat = 1 << iota
	xmdVersion
	xmdAtime
	xmdCksum
	xmdCksumMissingRecomp
	xmdCksumPresentRecomp
)

type (
	// local (and locally stored) object metadata along with associated runtime context
	objectXprops struct {
		// this object's runtime context
		t           *targetrunner
		bucketmd    *bucketMD
		atimeRespCh chan *atime.Response
		cksumcfg    *cmn.CksumConf
		bprops      *cmn.BucketProps
		// this object's names
		fqn             string
		bucket, objname string
		uname           string
		newfqn          string
		parsedFQN       fs.FQNparsed // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		// this object's props
		version  string
		atime    time.Time
		atimestr string
		size     int64
		nhobj    cksumValue
		// other flags
		bislocal     bool // the bucket (that contains this object) is local
		doesnotexist bool // the object does not exists (by fstat)
		misplaced    bool // the object is misplaced
		badchecksum  bool // this object has a bad checksum
	}
)

func (xmd *objectXprops) restoredReceived(props *objectXprops) {
	if props.version != "" {
		xmd.version = props.version
	}
	if !props.atime.IsZero() {
		xmd.atime = props.atime
	}
	if props.atimestr != "" {
		xmd.atimestr = props.atimestr
	}
	if props.size != 0 {
		xmd.size = props.size
	}
	xmd.nhobj = props.nhobj
	xmd.badchecksum = false
	xmd.doesnotexist = false
}

func (xmd *objectXprops) exists() bool     { return !xmd.doesnotexist }
func (xmd *objectXprops) lruEnabled() bool { return xmd.bucketmd.lruEnabled(xmd.bucket) }
func (xmd *objectXprops) String() string {
	var (
		a string
		s = fmt.Sprintf("%s/%s", xmd.bucket, xmd.objname)
	)
	if glog.V(4) {
		s += fmt.Sprintf("(%s)", xmd.fqn)
		if xmd.size != 0 {
			s += " size=" + cmn.B2S(xmd.size, 1)
		}
		if xmd.version != "" {
			s += " ver=" + xmd.version
		}
		if xmd.nhobj != nil {
			s += " " + xmd.nhobj.String()
		}
	}
	if xmd.doesnotexist {
		a = cmn.DoesNotExist
	}
	if xmd.misplaced {
		if a != "" {
			a += ", "
		}
		a += "locally misplaced"
	}
	if xmd.badchecksum {
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
func (xmd *objectXprops) fill(action int) (errstr string) {
	// init
	if xmd.bucket == "" || xmd.objname == "" || xmd.fqn == "" {
		if errstr = xmd._init(); errstr != "" {
			return
		}
	}
	//
	// actions
	//
	if action&xmdFstat != 0 {
		finfo, err := os.Stat(xmd.fqn)
		if err != nil {
			switch {
			case os.IsNotExist(err):
				xmd.doesnotexist = true
			default:
				errstr = fmt.Sprintf("Failed to fstat %s, err: %v", xmd, err)
				xmd.t.fshc(err, xmd.fqn)
			}
			return
		}
		xmd.size = finfo.Size()
	}
	if action&xmdVersion != 0 {
		var version []byte
		if version, errstr = GetXattr(xmd.fqn, cmn.XattrObjVersion); errstr != "" {
			return
		}
		xmd.version = string(version)
	}
	if action&xmdAtime != 0 { // FIXME: check
		xmd.atimestr, xmd.atime, _ = getatimerunner().FormatAtime(xmd.fqn, xmd.atimeRespCh, xmd.lruEnabled())
	}
	if action&xmdCksum != 0 {
		cksumAction := action&xmdCksumMissingRecomp | action&xmdCksumPresentRecomp
		if errstr = xmd._checksum(cksumAction); errstr != "" {
			return
		}
	}
	return
}

func (xmd *objectXprops) badChecksum(hksum cksumValue) (errstr string) {
	if xmd.nhobj != nil {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != %s)", xmd, hksum, xmd.nhobj)
	} else {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != nil)", xmd, hksum)
	}
	return
}

// xattrs
func (xmd *objectXprops) persist() (errstr string) {
	if xmd.nhobj != nil {
		_, hval := xmd.nhobj.get()
		if errstr = SetXattr(xmd.fqn, cmn.XattrXXHashVal, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if xmd.version != "" {
		errstr = SetXattr(xmd.fqn, cmn.XattrObjVersion, []byte(xmd.version))
	}
	if !xmd.atime.IsZero() && xmd.lruEnabled() {
		getatimerunner().Touch(xmd.fqn, xmd.atime)
	}
	return
}

// incObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (local bucket versioning (re)enabled, new file)
// the version is set to "1"
func (xmd *objectXprops) incObjectVersion() (newVersion string, errstr string) {
	const initialVersion = "1"
	if xmd.doesnotexist {
		newVersion = initialVersion
		return
	}

	var vbytes []byte
	if vbytes, errstr = GetXattr(xmd.fqn, cmn.XattrObjVersion); errstr != "" {
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
// private
//

func (xmd *objectXprops) _init() (errstr string) {
	// resolve fqn
	if xmd.bucket == "" || xmd.objname == "" {
		cmn.Assert(xmd.fqn != "")
		if errstr = xmd._resolveFQN(); errstr != "" {
			return
		}
		xmd.bucket, xmd.objname = xmd.parsedFQN.Bucket, xmd.parsedFQN.Objname
		if xmd.bucket == "" || xmd.objname == "" {
			return
		}
	}
	xmd.uname = cluster.Uname(xmd.bucket, xmd.objname)
	// bucketmd, bislocal, bprops
	xmd.bucketmd = xmd.t.bmdowner.get()
	xmd.bislocal = xmd.bucketmd.IsLocal(xmd.bucket)
	xmd.bprops, _ = xmd.bucketmd.get(xmd.bucket, xmd.bislocal)
	// cksumcfg
	xmd.cksumcfg = &cmn.GCO.Get().Cksum
	if xmd.bprops != nil && xmd.bprops.Checksum != cmn.ChecksumInherit {
		xmd.cksumcfg = &xmd.bprops.CksumConf
	}
	if xmd.fqn == "" {
		xmd.fqn, errstr = cluster.FQN(fs.ObjectType, xmd.bucket, xmd.objname, xmd.bislocal)
	}
	if xmd.parsedFQN.Bucket == "" || xmd.parsedFQN.Objname == "" {
		errstr = xmd._resolveFQN()
	}
	return
}

func (xmd *objectXprops) _resolveFQN() (errstr string) {
	var (
		err    error
		newfqn string
	)
	xmd.parsedFQN, newfqn, err = cluster.ResolveFQN(xmd.fqn, xmd.t.bmdowner)
	if err != nil {
		if _, ok := err.(*cluster.ErrFqnMisplaced); ok {
			xmd.misplaced = true
			xmd.newfqn = newfqn
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
func (xmd *objectXprops) _checksum(action int) (errstr string) {
	var (
		storedCksum, computedCksum string
		algo                       = xmd.cksumcfg.Checksum
	)
	if xmd.cksumcfg.Checksum == cmn.ChecksumNone {
		return
	}
	cmn.Assert(algo == cmn.ChecksumXXHash, fmt.Sprintf("Unsupported checksum algorithm '%s'", algo))

	if xmd.nhobj != nil {
		_, storedCksum = xmd.nhobj.get()
	} else if storedCksum, errstr = getXattrCksum(xmd.fqn, algo); errstr != "" {
		xmd.t.fshc(errors.New(errstr), xmd.fqn)
		return
	} else if storedCksum != "" {
		xmd.nhobj = newCksum(algo, storedCksum)
	}
	if action == 0 {
		return
	}
	// compute
	if storedCksum == "" && action&xmdCksumMissingRecomp != 0 {
		if computedCksum, errstr = recomputeXXHash(xmd.fqn, xmd.size); errstr != "" {
			return
		}
		if errstr = SetXattr(xmd.fqn, cmn.XattrXXHashVal, []byte(computedCksum)); errstr != "" {
			xmd.nhobj = nil
			xmd.t.fshc(errors.New(errstr), xmd.fqn)
			return
		}
		xmd.nhobj = newCksum(algo, computedCksum)
		return
	}
	if storedCksum != "" && action&xmdCksumPresentRecomp != 0 {
		if computedCksum, errstr = recomputeXXHash(xmd.fqn, xmd.size); errstr != "" {
			return
		}
		v := newCksum(algo, computedCksum)
		if !eqCksum(xmd.nhobj, v) {
			xmd.badchecksum = true
			errstr = xmd.badChecksum(v)
		}
	}
	return
}

//
// typed checksum value
//
type (
	cksumValue interface {
		get() (string, string)
		String() string
	}
	cksumvalxxhash struct {
		kind string
		val  string
	}
	cksumvalmd5 struct {
		kind string
		val  string
	}
)

func newCksum(kind string, val string) cksumValue {
	if kind == "" {
		return nil
	}
	if val == "" {
		return nil
	}
	if kind == cmn.ChecksumXXHash {
		return cksumvalxxhash{kind, val}
	}
	cmn.Assert(kind == cmn.ChecksumMD5)
	return cksumvalmd5{kind, val}
}

func eqCksum(a, b cksumValue) bool {
	if a == nil || b == nil {
		return false
	}
	t1, v1 := a.get()
	t2, v2 := b.get()
	return t1 == t2 && v1 == v2
}

func (v cksumvalxxhash) get() (string, string) { return v.kind, v.val }
func (v cksumvalmd5) get() (string, string)    { return v.kind, v.val }
func (v cksumvalxxhash) String() string        { return "(" + v.kind + ", " + v.val[:8] + "...)" }
func (v cksumvalmd5) String() string           { return "(" + v.kind + ", " + v.val[:8] + "...)" }
