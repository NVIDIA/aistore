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
	"strings"
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

const copyNameSepa = "\"\""

type (
	lmeta struct {
		uname   string
		size    int64
		version string
		cksum   cmn.Cksummer
		atime   int64
		copyFQN []string
	}
	LOM struct {
		// local meta
		md lmeta
		// other names
		FQN             string
		Bucket, Objname string
		BucketProvider  string
		HrwFQN          string // misplaced?
		// runtime context
		T           Target
		config      *cmn.Config
		bucketMD    *BMD
		AtimeRespCh chan *atime.Response
		BckProps    *cmn.BucketProps
		bckID       int64
		ParsedFQN   fs.ParsedFQN // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		BckIsLocal  bool         // the bucket (that contains this object) is local
		BadCksum    bool         // this object has a bad checksum
		exists      bool         // determines if the object exists or not (initially set by fstat)
		loaded      bool
	}
)

//
// LOM public methods
//

func (lom *LOM) Uname() string               { return lom.md.uname }
func (lom *LOM) CopyFQN() []string           { return lom.md.copyFQN }
func (lom *LOM) Size() int64                 { return lom.md.size }
func (lom *LOM) SetSize(size int64)          { lom.md.size = size }
func (lom *LOM) Version() string             { return lom.md.version }
func (lom *LOM) SetVersion(ver string)       { lom.md.version = ver }
func (lom *LOM) Cksum() cmn.Cksummer         { return lom.md.cksum }
func (lom *LOM) SetCksum(cksum cmn.Cksummer) { lom.md.cksum = cksum }
func (lom *LOM) Atime() time.Time            { return time.Unix(0, lom.md.atime) }
func (lom *LOM) AtimeUnix() int64            { return lom.md.atime }
func (lom *LOM) SetAtimeUnix(tu int64)       { lom.md.atime = tu }
func (lom *LOM) ECEnabled() bool             { return lom.BckProps.EC.Enabled }
func (lom *LOM) LRUEnabled() bool            { return lom.BckProps.LRU.Enabled }
func (lom *LOM) Misplaced() bool             { return lom.HrwFQN != lom.FQN && !lom.IsCopy() } // misplaced (subj to rebalancing)
func (lom *LOM) HasCopies() bool             { return !lom.IsCopy() && lom.NumCopies() > 1 }
func (lom *LOM) NumCopies() int              { return len(lom.md.copyFQN) + 1 }
func (lom *LOM) IsCopy() bool {
	return len(lom.md.copyFQN) == 1 && lom.md.copyFQN[0] == lom.HrwFQN // is a local copy of an object
}
func (lom *LOM) Config() *cmn.Config {
	if lom.config == nil {
		lom.config = cmn.GCO.Get()
	}
	return lom.config
}
func (lom *LOM) MirrorConf() *cmn.MirrorConf { return &lom.BckProps.Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf {
	conf := &lom.BckProps.Cksum
	if conf.Type == cmn.PropInherit {
		conf = &lom.Config().Cksum
	}
	return conf
}
func (lom *LOM) VerConf() *cmn.VersionConf {
	conf := &lom.BckProps.Versioning
	if conf.Type == cmn.PropInherit {
		conf = &lom.Config().Ver
	}
	return conf
}
func (lom *LOM) RebalanceConf() *cmn.RebalanceConf { return &lom.BckProps.Rebalance }
func (lom *LOM) GenFQN(ty, prefix string) string {
	return fs.CSM.GenContentParsedFQN(lom.ParsedFQN, ty, prefix)
}

//
// local copy management
//
func (lom *LOM) SetXcopy(cpyfqn string) (errstr string) { // cross-ref
	var copies string
	if len(lom.md.copyFQN) == 0 {
		lom.md.copyFQN = []string{cpyfqn}
		copies = cpyfqn
	} else {
		lom.md.copyFQN = append(lom.md.copyFQN, cpyfqn)
		copies = strings.Join(lom.md.copyFQN, copyNameSepa)
	}
	if errstr = fs.SetXattr(lom.FQN, cmn.XattrCopies, []byte(copies)); errstr == "" {
		if errstr = fs.SetXattr(cpyfqn, cmn.XattrCopies, []byte(lom.FQN)); errstr == "" {
			return // ok
		}
	}
	// on error
	if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
		lom.T.FSHC(err, lom.FQN)
	}
	return
}

func (lom *LOM) DelCopy(cpyfqn string) (errstr string) {
	cmn.Assert(!lom.IsCopy())
	var (
		cpyidx = -1
		l      = len(lom.md.copyFQN)
	)
	for i := 0; i < l; i++ {
		if lom.md.copyFQN[i] == cpyfqn {
			cpyidx = i
			break
		}
	}
	if cpyidx < 0 {
		return fmt.Sprintf("lom %s(%d): copy %s %s", lom, l, cpyfqn, cmn.DoesNotExist)
	}
	if l == 1 {
		return lom.DelAllCopies()
	}
	if cpyidx < l-1 {
		copy(lom.md.copyFQN[cpyidx:], lom.md.copyFQN[cpyidx+1:])
	}
	lom.md.copyFQN = lom.md.copyFQN[:l-1]
	if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
		return err.Error()
	}
	copies := strings.Join(lom.md.copyFQN, copyNameSepa)
	errstr = fs.SetXattr(lom.FQN, cmn.XattrCopies, []byte(copies))
	return
}

func (lom *LOM) DelAllCopies() (errstr string) {
	cmn.Assert(!lom.IsCopy())
	if !lom.HasCopies() {
		return
	}
	n := 0
	for _, cpyfqn := range lom.md.copyFQN {
		if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
			errstr = err.Error()
			continue
		}
		n++
	}
	if n == 0 {
		return
	} else if n < len(lom.md.copyFQN) {
		glog.Errorf("%s: failed to remove copies(%d, %d), err: %s", lom, n, len(lom.md.copyFQN), errstr)
	}
	lom.md.copyFQN = []string{}
	errstr = fs.DelXattr(lom.FQN, cmn.XattrCopies)
	return
}

func (lom *LOM) CopyObject(dstFQN string, buf []byte) (dst *LOM, err error) {
	if lom.IsCopy() {
		err = fmt.Errorf("%s is a copy", lom)
		return
	}
	dst = lom.clone(dstFQN)
	if err = cmn.CopyFile(lom.FQN, dst.FQN, buf); err != nil {
		return
	}
	if errstr := dst.PersistCksumVer(); errstr != "" {
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
		if lom.md.size != 0 {
			s += " size=" + cmn.B2S(lom.md.size, 1)
		}
		if lom.md.version != "" {
			s += " ver=" + lom.md.version
		}
		if lom.md.cksum != nil {
			s += " " + lom.md.cksum.String()
		}
	}
	if !lom.loaded {
		a = "(uninitialized)"
	} else if !lom.Exists() {
		a = "(x)"
	} else {
		if lom.Misplaced() {
			a += "(misplaced)"
		}
		if lom.IsCopy() {
			a += "(is-copy)"
		}
		if lom.HasCopies() {
			a += "(has-copies)"
		}
		if lom.BadCksum {
			a += "(bad-checksum)"
		}
	}
	return s + a + "]"
}

func (lom *LOM) BadCksumErr(cksum cmn.Cksummer) (errstr string) {
	if lom.md.cksum != nil {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != %s)", lom, cksum, lom.md.cksum)
	} else {
		errstr = fmt.Sprintf("BAD CHECKSUM: %s (%s != nil)", lom, cksum)
	}
	return
}

// xattrs: cmn.XattrXXHash and cmn.XattrVersion
// NOTE:
// - cmn.XattrCopies is updated separately by the 2-way mirroring code
// - atime is also updated explicitly via UpdateAtime()
func (lom *LOM) PersistCksumVer() (errstr string) {
	if lom.md.cksum != nil {
		_, cksumValue := lom.md.cksum.Get()
		if errstr = fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(cksumValue)); errstr != "" {
			return errstr
		}
	}
	if lom.md.version != "" {
		errstr = fs.SetXattr(lom.FQN, cmn.XattrVersion, []byte(lom.md.version))
	}
	return
}

func (lom *LOM) UpdateAtimeUnix(at int64, migrated bool) {
	lom.md.atime = at
	if at == 0 {
		return
	}
	if lom.LRUEnabled() || migrated {
		ratime := lom.T.GetAtimeRunner()
		ratime.Touch(lom.ParsedFQN.MpathInfo.Path, lom.FQN, lom.Atime())
	}
}

// IncObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (local bucket versioning (re)enabled, new file)
// the version is set to "1"
func (lom *LOM) IncObjectVersion() (newVersion string, errstr string) {
	const initialVersion = "1"
	if !lom.exists {
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

// best-effort GET load balancing (see also `mirror` for loadBalancePUT)
func (lom *LOM) LoadBalanceGET() (fqn string) {
	fqn = lom.FQN
	if len(lom.md.copyFQN) == 0 {
		return
	}
	var mp *fs.MountpathInfo
	_, u := lom.ParsedFQN.MpathInfo.GetIOstats(fs.StatDiskUtil)
	umin := u
	for _, cpyfqn := range lom.md.copyFQN {
		parsedCpyFQN, err := fs.Mountpaths.FQN2Info(cpyfqn)
		if err != nil {
			glog.Errorln(err)
			return
		}
		_, uc := parsedCpyFQN.MpathInfo.GetIOstats(fs.StatDiskUtil)
		if uc.Max < u.Max-float32(lom.MirrorConf().UtilThresh) && uc.Min <= u.Min {
			if uc.Max < umin.Max && uc.Min <= umin.Min {
				fqn = cpyfqn
				umin = uc
				mp = parsedCpyFQN.MpathInfo
			}
		}
	}
	if mp != nil && bool(glog.V(4)) {
		glog.Infof("GET %s from a mirror %s", lom, mp)
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
func (lom *LOM) ValidateChecksum(recompute bool) (errstr string) {
	var (
		storedCksum, computedCksum string
		cksumType                  = lom.CksumConf().Type
	)
	if cksumType == cmn.ChecksumNone {
		return
	}
	if lom.md.cksum != nil {
		_, storedCksum = lom.md.cksum.Get()
		cmn.Assert(storedCksum != "")
	}
	{ // FIXME: single xattr-meta
		var b []byte
		cmn.Assert(cksumType == cmn.ChecksumXXHash)
		if b, errstr = fs.GetXattr(lom.FQN, cmn.XattrXXHash); errstr != "" {
			lom.BadCksum = true
			return
		}
		if !recompute && lom.md.cksum == nil && len(b) == 0 {
			return
		}
		v := cmn.NewCksum(cksumType, string(b))
		if !cmn.EqCksum(lom.md.cksum, v) {
			lom.BadCksum = true
			errstr = lom.BadCksumErr(v)
			lom.Uncache()
			return
		}
	}
	if storedCksum != "" && !recompute {
		return
	}
	// compute
	cmn.Assert(cksumType == cmn.ChecksumXXHash) // sha256 et al. not implemented yet
	if computedCksum, errstr = lom.computeXXHash(lom.FQN, lom.md.size); errstr != "" {
		return
	}
	if storedCksum == "" {
		if errstr = fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(computedCksum)); errstr != "" {
			lom.md.cksum = nil
			lom.T.FSHC(errors.New(errstr), lom.FQN)
			return
		}
		lom.md.cksum = cmn.NewCksum(cksumType, computedCksum)
		lom.ReCache()
		return
	}
	v := cmn.NewCksum(cksumType, computedCksum)
	if !cmn.EqCksum(lom.md.cksum, v) {
		lom.BadCksum = true
		errstr = lom.BadCksumErr(v)
		lom.Uncache()
	}
	return
}

func (lom *LOM) CksumComputeIfMissing() (cksum cmn.Cksummer, errstr string) {
	var (
		val       string
		cksumType = lom.CksumConf().Type
	)
	if cksumType == cmn.ChecksumNone {
		return
	}
	if lom.md.cksum != nil {
		cksum = lom.md.cksum
		return
	}
	val, errstr = lom.computeXXHash(lom.FQN, lom.md.size)
	if errstr != "" {
		return
	}
	cksum = cmn.NewCksum(cmn.ChecksumXXHash, val)
	return
}

func (lom *LOM) computeXXHash(fqn string, size int64) (cksumstr, errstr string) {
	file, err := os.Open(fqn)
	if err != nil {
		errstr = fmt.Sprintf("%s, err: %v", fqn, err)
		return
	}
	buf, slab := lom.T.GetMem2().AllocFromSlab2(size)
	cksumstr, errstr = cmn.ComputeXXHash(file, buf)
	file.Close()
	slab.Free(buf)
	return
}

//
// private methods
//
func (lom *LOM) clone(fqn string) *LOM {
	dst := &LOM{}
	*dst = *lom
	cmn.Assert(dst.md.uname == lom.md.uname) // DEBUG
	dst.FQN = fqn
	dst.init("")
	return dst
}

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
			bckProvider = cmn.BckProviderFromLocal(lom.ParsedFQN.IsLocal)
		}
	}

	lom.md.uname = Uname(lom.Bucket, lom.Objname)
	// bucketmd, bckIsLocal, bprops
	lom.bucketMD = bowner.Get()
	if err := lom.initBckIsLocal(bckProvider); err != nil {
		return err.Error()
	}
	lom.BckProps, _ = lom.bucketMD.Get(lom.Bucket, lom.BckIsLocal)
	if lom.FQN == "" {
		lom.FQN, lom.ParsedFQN.Digest, errstr = FQN(fs.ObjectType, lom.Bucket, lom.Objname, lom.BckIsLocal)
	}
	if lom.ParsedFQN.Bucket == "" || lom.ParsedFQN.Objname == "" {
		errstr = lom.resolveFQN(nil, lom.BckIsLocal)
	}
	cmn.Assert(lom.ParsedFQN.Digest != 0)
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
			return fmt.Errorf("%s local bucket %s for local provider", lom.Bucket, cmn.DoesNotExist)
		}
		lom.BckIsLocal = true
	} else {
		lom.BckIsLocal = lom.bucketMD.IsLocal(lom.Bucket)
	}
	return nil
}

//
// lom cache
//
func (lom *LOM) hkey() (string, int) {
	cmn.Assert(lom.ParsedFQN.Digest != 0) // DEBUG
	return lom.md.uname, int(lom.ParsedFQN.Digest & fs.LomCacheMask)
}
func (newlom LOM) Init(config ...*cmn.Config) (lom *LOM, errstr string) {
	lom = &newlom
	if errstr = lom.init(lom.BucketProvider); errstr != "" {
		return
	}
	if len(config) > 0 {
		lom.config = config[0]
	}
	return
}

func (lom *LOM) Load(add bool) (errstr string) {
	lom.loaded = true
	if lom.bckID == 0 {
		lom.bckID = lom.BckProps.BID
	}
	// fast path
	var (
		hkey, idx = lom.hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
	)
	if md, ok := cache.M.Load(hkey); ok {
		lmeta := md.(*lmeta)
		lom.md = *lmeta
		if !add { // uncache
			cache.M.Delete(hkey)
		}
		lom.exists = true
		return
	}
	// slow path
	errstr = lom.FromFS()
	if errstr == "" && lom.Exists() {
		md := &lmeta{}
		*md = lom.md
		if !add { // ditto
			return
		}
		cache.M.Store(hkey, md)
	}
	return
}

func (lom *LOM) Exists() bool {
	cmn.Assert(lom.loaded)
	// TODO: remove lom.BckIsLocal - same check should work for CBmap
	if lom.BckIsLocal && lom.exists {
		if !lom.bucketMD.Exists(lom.Bucket, lom.bckID, lom.BckIsLocal) {
			lom.Uncache()
			return false
		}
	}
	return lom.exists
}

func (lom *LOM) ReCache() {
	var (
		hkey, idx = lom.hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
		md        = &lmeta{}
	)
	*md = lom.md
	cache.M.Store(hkey, md)
	lom.loaded = true
}

func (lom *LOM) Uncache() {
	var (
		hkey, idx = lom.hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
	)
	cache.M.Delete(hkey)
	lom.loaded = false
}

func (lom *LOM) FromFS() (errstr string) {
	var (
		finfo              os.FileInfo
		err                error
		timeunix           int64
		version, b, cpyfqn []byte
	)
	// fstat
	lom.exists = true
	finfo, err = os.Stat(lom.FQN)
	if err != nil {
		lom.exists = false
		if !os.IsNotExist(err) {
			errstr = fmt.Sprintf("%s: fstat err: %v", lom, err)
			lom.T.FSHC(err, lom.FQN)
		}
		return
	}
	lom.md.size = finfo.Size()
	// version
	if version, errstr = fs.GetXattr(lom.FQN, cmn.XattrVersion); errstr != "" {
		return
	}
	lom.md.version = string(version)
	// atime
	timeunix, err = lom.T.GetAtimeRunner().GetAtime(lom.FQN, lom.ParsedFQN.MpathInfo.Path, lom.AtimeRespCh, lom.LRUEnabled())
	if err != nil {
		return err.Error()
	}
	lom.SetAtimeUnix(timeunix)
	// cksum
	if cksumType := lom.CksumConf().Type; cksumType != cmn.ChecksumNone {
		cmn.Assert(cksumType == cmn.ChecksumXXHash)
		if b, errstr = fs.GetXattr(lom.FQN, cmn.XattrXXHash); errstr != "" {
			return
		}
		lom.md.cksum = cmn.NewCksum(cksumType, string(b))
	}
	// copies
	if cpyfqn, errstr = fs.GetXattr(lom.FQN, cmn.XattrCopies); errstr != "" {
		return
	}
	if len(cpyfqn) > 0 && !lom.IsCopy() {
		lom.md.copyFQN = strings.Split(string(cpyfqn), copyNameSepa)
	}
	return
}

//
// evict lom cache
//
func EvictCache(bucket string) {
	availablePaths, _ := fs.Mountpaths.Get()
	var (
		b      = bucket + "/"
		l      = len(availablePaths) * (fs.LomCacheMask + 1)
		caches = make([]*fs.LomCache, l)
	)
	i := 0
	for _, mpathInfo := range availablePaths {
		for idx := 0; idx <= fs.LomCacheMask; idx++ {
			cache := mpathInfo.LomCache(idx)
			caches[i] = cache
			i++
		}
	}
	for _, cache := range caches {
		fevict := func(hkey, value interface{}) bool {
			uname := hkey.(string)
			if strings.HasPrefix(uname, b) {
				cache.M.Delete(hkey)
			}
			return true
		}
		cache.M.Range(fevict)
	}
}
