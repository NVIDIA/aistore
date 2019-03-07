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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
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
	LOMCopyProps struct {
		FQN            string
		Cksum          cmn.CksumProvider
		Version        string
		BucketProvider string
		Atime          time.Time
	}

	LOM struct {
		// this object's runtime context
		T           Target
		bucketMD    *BMD
		AtimeRespCh chan *atime.Response
		Config      *cmn.Config
		CksumConf   *cmn.CksumConf
		MirrorConf  *cmn.MirrorConf
		BckProps    *cmn.BucketProps
		// names
		FQN             string
		Bucket, Objname string
		Uname           string
		HrwFQN          string       // misplaced?
		CopyFQN         string       // local replica
		ParsedFQN       fs.ParsedFQN // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		// props
		Version  string
		Atime    time.Time
		Size     int64
		Cksum    cmn.CksumProvider
		// flags
		BckIsLocal bool // the bucket (that contains this object) is local
		BadCksum   bool // this object has a bad checksum
		exists     bool // determines if the object exists or not (initially set by fstat)
	}
)

func (lom *LOM) RestoredReceived(props *LOM) {
	if props.Version != "" {
		lom.Version = props.Version
	}
	if !props.Atime.IsZero() {
		lom.Atime = props.Atime
	}
	if props.Size != 0 {
		lom.Size = props.Size
	}
	lom.Cksum = props.Cksum
	lom.BadCksum = false
	lom.SetExists(true)
}

func (lom *LOM) SetExists(exists bool) { lom.exists = exists }
func (lom *LOM) Exists() bool          { return lom.exists }
func (lom *LOM) LRUEnabled() bool      { return lom.BckProps.LRU.Enabled }
func (lom *LOM) Misplaced() bool       { return lom.HrwFQN != lom.FQN && !lom.IsCopy() }         // misplaced (subj to rebalancing)
func (lom *LOM) IsCopy() bool          { return lom.CopyFQN != "" && lom.CopyFQN == lom.HrwFQN } // is a mirrored copy of an object
func (lom *LOM) HasCopy() bool         { return lom.CopyFQN != "" && lom.FQN == lom.HrwFQN }     // has one mirrored copy

func (lom *LOM) GenFQN(ty, prefix string) string {
	return fs.CSM.GenContentParsedFQN(lom.ParsedFQN, ty, prefix)
}

func (lom *LOM) Atimestr(format ...string) string {
	f := time.RFC822

	if len(format) > 0 {
		f = format[0]
	}

	return lom.Atime.Format(f)
}

func (lom *LOM) Copy(props LOMCopyProps) *LOM {
	dstLOM := &LOM{}
	*dstLOM = *lom

	if dstLOM.Cksum != nil && props.Cksum == nil {
		_ = dstLOM.checksum(0) // already copied; ignoring "get" errors at this point
	} else if props.Cksum != nil {
		dstLOM.Cksum = props.Cksum
	}
	if props.Version != "" {
		dstLOM.Version = props.Version
	}

	if props.FQN != "" {
		dstLOM.Bucket = ""
		dstLOM.Objname = ""
		dstLOM.FQN = props.FQN
		dstLOM.init(props.BucketProvider)
	}
	dstLOM.Atime = props.Atime

	return dstLOM
}

//
// local replica management
//
func (lom *LOM) SetXcopy(cpyfqn string) (errstr string) { // cross-ref
	if errstr = fs.SetXattr(lom.FQN, cmn.XattrCopies, []byte(cpyfqn)); errstr == "" {
		if errstr = fs.SetXattr(cpyfqn, cmn.XattrCopies, []byte(lom.FQN)); errstr == "" {
			lom.CopyFQN = cpyfqn
			return
		}
	}
	if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.FQN)
	}
	return
}

func (lom *LOM) DelCopy() (errstr string) {
	if err := os.Remove(lom.CopyFQN); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.FQN)
		return err.Error()
	}
	errstr = fs.DelXattr(lom.FQN, cmn.XattrCopies)
	return
}

func (lom *LOM) CopyObject(dstFQN string, buf []byte) (err error) {
	dstLOM := lom.Copy(LOMCopyProps{FQN: dstFQN})
	if err = cmn.CopyFile(lom.FQN, dstLOM.FQN, buf); err != nil {
		return
	}
	if errstr := dstLOM.Persist(false); errstr != "" {
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
		s += fmt.Sprintf("(%s)", lom.FQN)
		if lom.Size != 0 {
			s += " size=" + cmn.B2S(lom.Size, 1)
		}
		if lom.Version != "" {
			s += " ver=" + lom.Version
		}
		if lom.Cksum != nil {
			s += " " + lom.Cksum.String()
		}
	}
	if !lom.Exists() {
		a = "(x)"
	} else {
		if lom.Misplaced() {
			a += "(misplaced)"
		}
		if lom.IsCopy() {
			a += "(is-copy)"
		}
		if lom.HasCopy() {
			a += "(has-copy)"
		}
		if lom.BadCksum {
			a += "(bad-checksum)"
		}
	}
	return s + a + "]"
}

// main method
func (lom *LOM) Fill(bckProvider string, action int, config ...*cmn.Config) (errstr string) {
	lom.SetExists(true) // by default we assume that the object exists
	if lom.Bucket == "" || lom.Objname == "" || lom.FQN == "" {
		if errstr = lom.init(bckProvider); errstr != "" {
			return
		}
		if len(config) > 0 {
			lom.Config = config[0]
		} else {
			lom.Config = cmn.GCO.Get()
		}
		cprovider := lom.Config.CloudProvider
		if !lom.BckIsLocal && (cprovider == "" || cprovider == cmn.ProviderAIS) {
			errstr = fmt.Sprintf("%s: cloud bucket with no cloud provider (%s)", lom, cprovider)
			return
		}
		lom.CksumConf = &lom.BckProps.Cksum
		if lom.CksumConf.Type == cmn.ChecksumInherit {
			lom.CksumConf = &lom.Config.Cksum
		}
		lom.MirrorConf = &lom.BckProps.Mirror
	}
	// [local copy] always enforce LomCopy if the following is true
	if (lom.Misplaced() || action&LomFstat != 0) && lom.MirrorConf.Copies != 0 {
		action |= LomCopy
	}
	//
	// actions
	//
	if action&LomFstat != 0 {
		finfo, err := os.Stat(lom.FQN)
		if err != nil {
			switch {
			case os.IsNotExist(err):
				lom.SetExists(false)
			default:
				errstr = fmt.Sprintf("Failed to fstat %s, err: %v", lom, err)
				lom.T.FSHC(err, lom.FQN)
			}
			return
		}
		lom.Size = finfo.Size()

	}
	if action&LomVersion != 0 {
		var version []byte
		if version, errstr = fs.GetXattr(lom.FQN, cmn.XattrVersion); errstr != "" {
			return
		}
		lom.Version = string(version)
	}
	if action&LomAtime != 0 { // FIXME: RFC822 format
		var err error
		_, lom.Atime, err = lom.T.GetAtimeRunner().FormatAtime(lom.FQN, lom.ParsedFQN.MpathInfo.Path, lom.AtimeRespCh, lom.LRUEnabled())

		if err != nil {
			errstr = err.Error()
			return
		}
	}
	if action&LomCksum != 0 {
		cksumAction := action&LomCksumMissingRecomp | action&LomCksumPresentRecomp
		if errstr = lom.checksum(cksumAction); errstr != "" {
			return
		}
	}
	if action&LomCopy != 0 {
		var copyfqn []byte
		if copyfqn, errstr = fs.GetXattr(lom.FQN, cmn.XattrCopies); errstr != "" {
			return
		}
		lom.CopyFQN = string(copyfqn)
	}
	return
}

func (lom *LOM) BadCksumErr(cksum cmn.CksumProvider) (errstr string) {
	if lom.Cksum != nil {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != %s)", lom, cksum, lom.Cksum)
	} else {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != nil)", lom, cksum)
	}
	return
}

// xattrs
func (lom *LOM) Persist(setAtime bool) (errstr string) {
	if lom.Cksum != nil {
		_, cksumValue := lom.Cksum.Get()
		if errstr = fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(cksumValue)); errstr != "" {
			return errstr
		}
	}
	if lom.Version != "" {
		errstr = fs.SetXattr(lom.FQN, cmn.XattrVersion, []byte(lom.Version))
	}
	if setAtime {
		lom.UpdateAtime(lom.Atime)
	}

	//       cmn.XattrCopies is also updated separately by the 2-way mirroring code
	return
}

func (lom *LOM) UpdateAtime(at time.Time) {
	lom.Atime = at
	if at.IsZero() {
		return
	}
	if !lom.LRUEnabled() {
		return
	}
	ratime := lom.T.GetAtimeRunner()
	ratime.Touch(lom.ParsedFQN.MpathInfo.Path, lom.FQN, at)
}

// IncObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (local bucket versioning (re)enabled, new file)
// the version is set to "1"
func (lom *LOM) IncObjectVersion() (newVersion string, errstr string) {
	const initialVersion = "1"
	if !lom.Exists() {
		newVersion = initialVersion
		return
	}

	var vbytes []byte
	if vbytes, errstr = fs.GetXattr(lom.FQN, cmn.XattrVersion); errstr != "" {
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
	fqn = lom.FQN
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
	if currRepl.Max < currMain.Max-float32(lom.MirrorConf.UtilThresh) && currRepl.Min <= currMain.Min {
		fqn = lom.CopyFQN
		if glog.V(4) {
			glog.Infof("GET %s from a mirror %s", lom, parsedCpyFQN.MpathInfo)
		}
	}
	return
}

//
// private methods
//

func (lom *LOM) init(bckProvider string) (errstr string) {
	bowner := lom.T.GetBowner()
	// resolve fqn
	if lom.Bucket == "" || lom.Objname == "" {
		cmn.Assert(lom.FQN != "")
		if errstr = lom.resolveFQN(bowner); errstr != "" {
			return
		}
		lom.Bucket, lom.Objname = lom.ParsedFQN.Bucket, lom.ParsedFQN.Objname
		if lom.Bucket == "" || lom.Objname == "" {
			return
		}

		if bckProvider == "" {
			bckProvider = GenBucketProvider(lom.ParsedFQN.IsLocal)
		}
	}

	lom.Uname = Uname(lom.Bucket, lom.Objname)
	// bucketmd, bckIsLocal, bprops
	lom.bucketMD = bowner.Get()
	if err := lom.initBckIsLocal(bckProvider); err != nil {
		return err.Error()
	}
	lom.BckProps, _ = lom.bucketMD.Get(lom.Bucket, lom.BckIsLocal)
	if lom.FQN == "" {
		lom.FQN, errstr = FQN(fs.ObjectType, lom.Bucket, lom.Objname, lom.BckIsLocal)
	}
	if lom.ParsedFQN.Bucket == "" || lom.ParsedFQN.Objname == "" {
		errstr = lom.resolveFQN(nil, lom.BckIsLocal)
	}
	return
}

func (lom *LOM) resolveFQN(bowner Bowner, bckIsLocal ...bool) (errstr string) {
	var err error
	if len(bckIsLocal) == 0 {
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN, bowner)
	} else {
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN, nil, lom.BckIsLocal)
	}
	if err != nil {
		errstr = err.Error()
	}
	return
}

func (lom *LOM) initBckIsLocal(bckProvider string) error {
	if bckProvider == cmn.CloudBs {
		lom.BckIsLocal = false
	} else if bckProvider == cmn.LocalBs {
		if !lom.bucketMD.IsLocal(lom.Bucket) {
			return fmt.Errorf("bucket provider set to 'local' but %s local bucket does not exist", lom.Bucket)
		}
		lom.BckIsLocal = true
	} else {
		lom.BckIsLocal = lom.bucketMD.IsLocal(lom.Bucket)
	}
	return nil
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
		cksumType                  = lom.CksumConf.Type
	)
	if cksumType == cmn.ChecksumNone {
		return
	}
	cmn.AssertMsg(cksumType == cmn.ChecksumXXHash, fmt.Sprintf("Unsupported checksum algorithm '%s'", cksumType))
	if lom.Cksum != nil {
		_, storedCksum = lom.Cksum.Get()
	} else if b, errstr = fs.GetXattr(lom.FQN, cmn.XattrXXHash); errstr != "" {
		lom.T.FSHC(errors.New(errstr), lom.FQN)
		return
	} else if b != nil {
		storedCksum = string(b)
		lom.Cksum = cmn.NewCksum(cksumType, storedCksum)
	} else {
		glog.Warningf("%s is not checksummed", lom)
	}
	if action == 0 {
		return
	}
	// compute
	if storedCksum == "" && action&LomCksumMissingRecomp != 0 {
		if computedCksum, errstr = lom.recomputeXXHash(lom.FQN, lom.Size); errstr != "" {
			return
		}
		if errstr = fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(computedCksum)); errstr != "" {
			lom.Cksum = nil
			lom.T.FSHC(errors.New(errstr), lom.FQN)
			return
		}
		lom.Cksum = cmn.NewCksum(cksumType, computedCksum)
		return
	}
	if storedCksum != "" && action&LomCksumPresentRecomp != 0 {
		if computedCksum, errstr = lom.recomputeXXHash(lom.FQN, lom.Size); errstr != "" {
			return
		}
		v := cmn.NewCksum(cksumType, computedCksum)
		if !cmn.EqCksum(lom.Cksum, v) {
			lom.BadCksum = true
			errstr = lom.BadCksumErr(v)
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

func GenBucketProvider(isLocal bool) string {
	if isLocal {
		return cmn.LocalBs
	}
	return cmn.CloudBs
}
