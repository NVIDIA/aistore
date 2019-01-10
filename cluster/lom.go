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
	LomCopy
)

type (
	LOM struct {
		// this object's runtime context
		T           Target
		Bucketmd    *BMD
		AtimeRespCh chan *atime.Response
		Config      *cmn.Config
		Cksumcfg    *cmn.CksumConf
		Mirror      *cmn.MirrorConf
		Bprops      *cmn.BucketProps
		// names
		Fqn             string
		Bucket, Objname string
		Uname           string
		HrwFQN          string       // misplaced?
		CopyFQN         string       // local replica
		ParsedFQN       fs.FQNparsed // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		// props
		Version  string
		Atime    time.Time
		Atimestr string
		Size     int64
		Nhobj    cmn.CksumValue
		// flags
		Bislocal     bool // the bucket (that contains this object) is local
		Doesnotexist bool // the object does not exists (by fstat)
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
func (lom *LOM) Misplaced() bool  { return lom.HrwFQN != lom.Fqn && !lom.IsCopy() }         // misplaced (subj to rebalancing)
func (lom *LOM) IsCopy() bool     { return lom.CopyFQN != "" && lom.CopyFQN == lom.HrwFQN } // is a
func (lom *LOM) HasCopy() bool    { return lom.CopyFQN != "" && lom.Fqn == lom.HrwFQN }     // has one

//
// local replica management
//
func (lom *LOM) SetXcopy(cpyfqn string) (errstr string) { // cross-ref
	if errstr = fs.SetXattr(lom.Fqn, cmn.XattrCopies, []byte(cpyfqn)); errstr == "" {
		if errstr = fs.SetXattr(lom.CopyFQN, cmn.XattrCopies, []byte(lom.Fqn)); errstr == "" {
			lom.CopyFQN = cpyfqn
			return
		}
	}
	if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.Fqn)
	}
	return
}

func (lom *LOM) DelCopy() (errstr string) {
	if err := os.Remove(lom.CopyFQN); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.Fqn)
		return err.Error()
	}
	errstr = fs.DelXattr(lom.Fqn, cmn.XattrCopies)
	return
}

func (lom *LOM) CopyObject(dstfqn string, buf []byte) (err error) {
	if err = cmn.CopyFile(lom.Fqn, dstfqn, buf); err != nil {
		return
	}
	lomdst := &LOM{}
	*lomdst = *lom
	lomdst.Fqn = dstfqn
	if lom.Nhobj == nil {
		_ = lom.checksum(0) // already copied; ignoring "get" errors at this point
	}
	lomdst.Nhobj = lom.Nhobj
	if lom.Version == "" {
		version, _ := fs.GetXattr(lom.Fqn, cmn.XattrVersion)
		lom.Version = string(version)
	}
	if errstr := lomdst.Persist(); errstr != "" {
		err = errors.New(errstr)
	}
	return
}

// format
func (lom *LOM) String() string {
	var (
		a string
		s = fmt.Sprintf("lom[%s/%s fs=%s", lom.Bucket, lom.Objname, lom.ParsedFQN.MpathInfo.FileSystem)
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
		a = "(" + cmn.DoesNotExist + ")"
	}
	if lom.Misplaced() {
		a += "(misplaced)"
	}
	if lom.IsCopy() {
		a += "(is a local replica)"
	}
	if lom.Badchecksum {
		a += "(bad checksum)"
	}
	return s + a + "]"
}

// main method
func (lom *LOM) Fill(action int, config ...*cmn.Config) (errstr string) {
	// init
	if lom.Bucket == "" || lom.Objname == "" || lom.Fqn == "" {
		if errstr = lom.init(); errstr != "" {
			return
		}
		if len(config) > 0 {
			lom.Config = config[0]
		} else {
			lom.Config = cmn.GCO.Get()
		}
		lom.Cksumcfg = &lom.Config.Cksum
		lom.Mirror = &lom.Config.Mirror
		if lom.Bprops != nil {
			if lom.Bprops.Checksum != cmn.ChecksumInherit {
				lom.Cksumcfg = &lom.Bprops.CksumConf
			}
			lom.Mirror = &lom.Bprops.MirrorConf
		}
	}
	// [local copy] always enforce LomCopy if the following is true
	if (lom.Misplaced() || action&LomFstat != 0) && lom.Bprops != nil && lom.Bprops.Copies != 0 {
		action |= LomCopy
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
		if version, errstr = fs.GetXattr(lom.Fqn, cmn.XattrVersion); errstr != "" {
			return
		}
		lom.Version = string(version)
	}
	if action&LomAtime != 0 { // FIXME: RFC822 format
		lom.Atimestr, lom.Atime, _ = lom.T.GetAtimeRunner().FormatAtime(lom.Fqn, lom.ParsedFQN.MpathInfo.Path, lom.AtimeRespCh, lom.LRUenabled())
	}
	if action&LomCksum != 0 {
		cksumAction := action&LomCksumMissingRecomp | action&LomCksumPresentRecomp
		if errstr = lom.checksum(cksumAction); errstr != "" {
			return
		}
	}
	if action&LomCopy != 0 {
		var copyfqn []byte
		if copyfqn, errstr = fs.GetXattr(lom.Fqn, cmn.XattrCopies); errstr != "" {
			return
		}
		lom.CopyFQN = string(copyfqn)
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
		if errstr = fs.SetXattr(lom.Fqn, cmn.XattrXXHash, []byte(hval)); errstr != "" {
			return errstr
		}
	}
	if lom.Version != "" {
		errstr = fs.SetXattr(lom.Fqn, cmn.XattrVersion, []byte(lom.Version))
	}
	// NOTE: atime is updated explicitly, via UpdateAtime() below
	//       cmn.XattrCopies is also updated separately by the 2-way mirroring code
	return
}

func (lom *LOM) UpdateAtime(at time.Time) {
	lom.Atime = at
	if at.IsZero() {
		return
	}
	lom.Atimestr = at.Format(cmn.RFC822) // TODO: add support for cmn.StampMicro
	if !lom.LRUenabled() {
		return
	}
	ratime := lom.T.GetAtimeRunner()
	ratime.Touch(lom.Fqn, at)
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
	if vbytes, errstr = fs.GetXattr(lom.Fqn, cmn.XattrVersion); errstr != "" {
		return
	}
	if currValue, err := strconv.Atoi(string(vbytes)); err != nil {
		newVersion = initialVersion
	} else {
		newVersion = fmt.Sprintf("%d", currValue+1)
	}
	return
}

// best effort load balancing (GET)
func (lom *LOM) ChooseMirror() (fqn string) {
	fqn = lom.Fqn
	if lom.CopyFQN == "" {
		return
	}
	parsedCpyFQN, err := fs.Mountpaths.FQN2Info(lom.CopyFQN)
	if err != nil {
		glog.Errorln(err)
		return
	}
	_, currMain := lom.ParsedFQN.MpathInfo.GetIOstats(fs.StatDiskUtil)
	_, currRepl := parsedCpyFQN.MpathInfo.GetIOstats(fs.StatDiskUtil)
	if currRepl.Max < currMain.Max-float32(lom.Mirror.MirrorUtilThresh) && currRepl.Min <= currMain.Min {
		fqn = lom.CopyFQN
		if glog.V(3) {
			glog.Infof("GET %s from a mirror %s", lom, parsedCpyFQN.MpathInfo)
		}
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
	if lom.Fqn == "" {
		lom.Fqn, errstr = FQN(fs.ObjectType, lom.Bucket, lom.Objname, lom.Bislocal)
	}
	if lom.ParsedFQN.Bucket == "" || lom.ParsedFQN.Objname == "" {
		errstr = lom.resolveFQN(nil, lom.Bislocal)
	}
	return
}

func (lom *LOM) resolveFQN(bowner Bowner, bislocal ...bool) (errstr string) {
	var err error
	if len(bislocal) == 0 {
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.Fqn, bowner)
	} else {
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.Fqn, nil, lom.Bislocal)
	}
	if err != nil {
		errstr = err.Error()
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
		b                          []byte
		algo                       = lom.Cksumcfg.Checksum
	)
	if lom.Cksumcfg.Checksum == cmn.ChecksumNone {
		return
	}
	cmn.Assert(algo == cmn.ChecksumXXHash, fmt.Sprintf("Unsupported checksum algorithm '%s'", algo))
	if lom.Nhobj != nil {
		_, storedCksum = lom.Nhobj.Get()
	} else if b, errstr = fs.GetXattr(lom.Fqn, cmn.XattrXXHash); errstr != "" {
		lom.T.FSHC(errors.New(errstr), lom.Fqn)
		return
	} else if b != nil {
		storedCksum = string(b)
		lom.Nhobj = cmn.NewCksum(algo, storedCksum)
	} else {
		glog.Warningf("%s is not checksummed", lom)
	}
	if action == 0 {
		return
	}
	// compute
	if storedCksum == "" && action&LomCksumMissingRecomp != 0 {
		if computedCksum, errstr = lom.recomputeXXHash(lom.Fqn, lom.Size); errstr != "" {
			return
		}
		if errstr = fs.SetXattr(lom.Fqn, cmn.XattrXXHash, []byte(computedCksum)); errstr != "" {
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
