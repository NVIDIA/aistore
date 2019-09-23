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
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/memsys"
)

//
// Local Object Metadata (LOM) is a locally stored object metadata comprising:
// - version, atime, checksum, size, etc. object attributes and flags
// - user and internally visible object names
// - associated runtime context including properties and configuration of the
//   bucket that contains the object, etc.
//

const pkgName = "cluster"

type (
	// NOTE: sizeof(lmeta) = 72 as of 4/16
	lmeta struct {
		uname   string
		version string
		size    int64
		atime   int64
		atimefs int64
		bckID   uint64
		cksum   *cmn.Cksum // ReCache(ref)
		copies  fs.MPI     // ditto
	}
	LOM struct {
		md      lmeta  // local meta
		bck     Bck    // bucket
		FQN     string // fqn
		Objname string // object name in the bucket
		HrwFQN  string // (misplaced?)
		// runtime
		T         Target
		config    *cmn.Config
		ParsedFQN fs.ParsedFQN // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		BadCksum  bool         // this object has a bad checksum
		exists    bool         // determines if the object exists or not (initially set by fstat)
		loaded    bool
	}
)

var (
	lomLocker nameLocker
)

func init() {
	lomLocker = make(nameLocker, fs.LomCacheMask+1)
	lomLocker.init()
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleCluster, logLvl)
	}
}

//
// LOM public methods
//

func (lom *LOM) Uname() string             { return lom.md.uname }
func (lom *LOM) Size() int64               { return lom.md.size }
func (lom *LOM) SetSize(size int64)        { lom.md.size = size }
func (lom *LOM) Version() string           { return lom.md.version }
func (lom *LOM) SetVersion(ver string)     { lom.md.version = ver }
func (lom *LOM) Cksum() *cmn.Cksum         { return lom.md.cksum }
func (lom *LOM) SetCksum(cksum *cmn.Cksum) { lom.md.cksum = cksum }
func (lom *LOM) Atime() time.Time          { return time.Unix(0, lom.md.atime) }
func (lom *LOM) AtimeUnix() int64          { return lom.md.atime }
func (lom *LOM) SetAtimeUnix(tu int64)     { lom.md.atime = tu }
func (lom *LOM) ECEnabled() bool           { return lom.Bprops().EC.Enabled }
func (lom *LOM) LRUEnabled() bool          { return lom.Bprops().LRU.Enabled }
func (lom *LOM) Misplaced() bool           { return lom.HrwFQN != lom.FQN } // subj to resilvering
func (lom *LOM) SetBID(bid uint64)         { lom.md.bckID, lom.bck.Props.BID = bid, bid }
func (lom *LOM) Bucket() string            { return lom.bck.Name }
func (lom *LOM) Bprops() *cmn.BucketProps  { return lom.bck.Props }
func (lom *LOM) IsAIS() bool               { return lom.bck.IsAIS() }
func (lom *LOM) Bck() *Bck                 { return &lom.bck }

//
// access perms
//
func (lom *LOM) AllowGET() error     { return lom.bck.AllowGET() }
func (lom *LOM) AllowHEAD() error    { return lom.bck.AllowHEAD() }
func (lom *LOM) AllowPUT() error     { return lom.bck.AllowPUT() }
func (lom *LOM) AllowColdGET() error { return lom.bck.AllowColdGET() }
func (lom *LOM) AllowDELETE() error  { return lom.bck.AllowDELETE() }
func (lom *LOM) AllowRENAME() error  { return lom.bck.AllowRENAME() }

func (lom *LOM) Config() *cmn.Config {
	if lom.config == nil {
		lom.config = cmn.GCO.Get()
	}
	return lom.config
}
func (lom *LOM) MirrorConf() *cmn.MirrorConf { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf {
	conf := &lom.Bprops().Cksum
	if conf.Type == cmn.PropInherit {
		conf = &lom.Config().Cksum
	}
	return conf
}
func (lom *LOM) VerConf() *cmn.VersionConf {
	conf := &lom.Bprops().Versioning
	if conf.Type == cmn.PropInherit {
		conf = &lom.Config().Ver
	}
	return conf
}

//
// local copy management
// TODO -- FIXME: lom.md.copies are not protected and are subject to races
//                the caller to use lom.Lock/Unlock (as in mirror)
//
func (lom *LOM) HasCopies() bool   { return !lom.IsCopy() && lom.NumCopies() > 1 }
func (lom *LOM) NumCopies() int    { return len(lom.md.copies) + 1 }
func (lom *LOM) GetCopies() fs.MPI { return lom.md.copies }
func (lom *LOM) IsCopy() bool {
	if len(lom.md.copies) != 1 {
		return false
	}
	_, ok := lom.md.copies[lom.HrwFQN]
	return ok
}
func (lom *LOM) SetCopies(cpyfqn string, mpi *fs.MountpathInfo) {
	lom.md.copies = make(fs.MPI, 1)
	lom.md.copies[cpyfqn] = mpi
}
func (lom *LOM) AddCopy(cpyfqn string, mpi *fs.MountpathInfo) {
	if lom.md.copies == nil {
		lom.SetCopies(cpyfqn, mpi)
	} else {
		lom.md.copies[cpyfqn] = mpi
	}
}
func (lom *LOM) DelCopy(cpyfqn string) (err error) {
	l := len(lom.md.copies)
	if _, ok := lom.md.copies[cpyfqn]; !ok {
		return fmt.Errorf("lom %s(%d): copy %s %s", lom, l, cpyfqn, cmn.DoesNotExist)
	}
	if l == 1 {
		return lom.DelAllCopies()
	}
	if lom._whingeCopy() {
		return
	}
	delete(lom.md.copies, cpyfqn)
	if err := os.Remove(cpyfqn); err != nil && !os.IsNotExist(err) {
		return err
	}
	return
}
func (lom *LOM) _whingeCopy() (yes bool) {
	if !lom.IsCopy() {
		return
	}
	msg := fmt.Sprintf("unexpected: %s([fqn=%s] [hrw=%s] %+v)", lom.StringEx(), lom.FQN, lom.HrwFQN, lom.md.copies)
	cmn.DassertMsg(false, msg, pkgName)
	glog.Error(msg)
	return true
}

func (lom *LOM) DelAllCopies() (err error) {
	if lom._whingeCopy() {
		return
	}
	if !lom.HasCopies() {
		return
	}
	n := 0
	for cpyfqn := range lom.md.copies {
		if err1 := os.Remove(cpyfqn); err1 != nil && !os.IsNotExist(err1) {
			err = err1
			continue
		}
		delete(lom.md.copies, cpyfqn)
		n++
	}
	if n < len(lom.md.copies) {
		glog.Errorf("%s: failed to remove some copies(%d < %d), err: %s", lom, n, len(lom.md.copies), err)
	} else {
		lom.md.copies = nil
	}
	return
}

func (lom *LOM) DelExtraCopies() (n int, err error) {
	if lom._whingeCopy() {
		return
	}
	availablePaths, _ := fs.Mountpaths.Get()
	for _, mpathInfo := range availablePaths {
		cpyfqn := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, lom.Bucket(), lom.bck.Provider, lom.Objname)
		if _, ok := lom.md.copies[cpyfqn]; ok {
			continue
		}
		if er := os.Remove(cpyfqn); er == nil {
			n++
		} else if !os.IsNotExist(er) {
			err = er
		}
	}
	return
}

func (lom *LOM) CopyObjectFromAny() (copied bool) {
	availablePaths, _ := fs.Mountpaths.Get()
	workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
	buf, slab := lom.T.GetMem2().AllocDefault()
	for path, mpathInfo := range availablePaths {
		if path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		fqn := fs.CSM.FQN(mpathInfo, lom.ParsedFQN.ContentType, lom.Bucket(), lom.bck.Provider, lom.Objname)
		if _, err := os.Stat(fqn); err != nil {
			continue
		}
		src := lom.Clone(fqn)

		lom.Lock(true)
		_, err := src.CopyObject(lom.FQN, workFQN, buf, false, true)
		lom.Unlock(true)

		if err == nil {
			copied = true
			break
		}
	}
	slab.Free(buf)
	return
}

// NOTE: caller is responsible for locking
func (lom *LOM) CopyObject(dstFQN, workFQN string, buf []byte, dstIsCopy, srcCopyOK bool) (dst *LOM, err error) {
	var written int64
	if !srcCopyOK && lom.IsCopy() {
		err = fmt.Errorf("%s is a mirrored copy", lom)
		return
	}
	written, err = cmn.CopyFile(lom.FQN, workFQN, buf)
	if err != nil {
		return
	}
	dst = lom.Clone(dstFQN)
	if err = cmn.Rename(workFQN, dstFQN); err != nil {
		if errRemove := os.Remove(workFQN); errRemove != nil {
			glog.Errorf("nested err: %v", errRemove)
		}
		return
	}
	if dstIsCopy {
		return
	}
	// TODO -- FIXME: md.size and md.copies
	dst.SetSize(written)
	dst.SetCksum(lom.Cksum())
	dst.SetVersion(lom.Version())
	dst.SetAtimeUnix(time.Now().UnixNano())
	if err = dst.Persist(); err != nil {
		if errRemove := os.Remove(dstFQN); errRemove != nil {
			glog.Errorf("nested err: %v", errRemove)
		}
	}
	return
}

//
// lom.String() and helpers
//

func (lom *LOM) String() string   { return lom._string(lom.Bucket()) }
func (lom *LOM) StringEx() string { return lom._string(lom.bck.String()) }

func (lom *LOM) _string(b string) string {
	var (
		a string
		s = fmt.Sprintf("o[%s/%s fs=%s", b, lom.Objname, lom.ParsedFQN.MpathInfo.FileSystem)
	)
	if glog.FastV(4, glog.SmoduleCluster) {
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
		a = "(-)"
	} else if !lom.exists {
		a = "(x)"
	} else {
		if lom.Misplaced() {
			a += "(misplaced)"
		}
		if lom.IsCopy() {
			a += "(copy)"
		}
		if n := lom.NumCopies(); n > 1 {
			a += fmt.Sprintf("(%dc)", n)
		}
		if lom.BadCksum {
			a += "(bad-cksum)"
		}
	}
	return s + a + "]"
}

func (lom *LOM) BadCksumErr(cksum *cmn.Cksum) (err error) {
	return errors.New(cmn.BadCksum(cksum, lom.md.cksum) + ", " + lom.StringEx())
}

// IncObjectVersion increments the current version xattrs and returns the new value.
// If the current version is empty (ais bucket versioning (re)enabled, new file)
// the version is set to "1"
func (lom *LOM) IncObjectVersion() (newVersion string, err error) {
	const initialVersion = "1"
	if !lom.exists {
		newVersion = initialVersion
		return
	}
	md, err := lom.lmfs(false)
	if err != nil {
		return "", err
	}
	if md.version == "" {
		return initialVersion, nil
	}
	numVersion, err := strconv.Atoi(md.version)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", numVersion+1), nil
}

// best-effort GET load balancing (see also mirror.findLeastUtilized())
func (lom *LOM) LoadBalanceGET(now time.Time) (fqn string) {
	if len(lom.md.copies) == 0 {
		return lom.FQN
	}
	return fs.Mountpaths.LoadBalanceGET(lom.FQN, lom.ParsedFQN.MpathInfo.Path, lom.md.copies, now)
}

// Returns stored checksum (if present) and computed checksum (if requested)
// MAY compute and store a missing (xxhash) checksum.
// If xattr checksum is different than lom's metadata checksum, returns error
// and do not recompute checksum even if recompute set to true.
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

// ValidateMetaChecksum validates whether checksum stored in lom's in-memory metadata
// matches checksum stored on disk.
// Use lom.ValidateContentChecksum() to recompute and check object's content checksum.
func (lom *LOM) ValidateMetaChecksum() error {
	var (
		cksumFromFS *cmn.Cksum
		md          *lmeta
		err         error
	)
	if lom.CksumConf().Type == cmn.ChecksumNone {
		return nil
	}
	md, err = lom.lmfs(false)
	if err != nil {
		return err
	}
	if md != nil {
		cksumFromFS = md.cksum
	}
	// different versions may have different checksums
	if (cksumFromFS != nil || lom.md.cksum != nil) && md.version == lom.md.version {
		if !cmn.EqCksum(lom.md.cksum, cksumFromFS) {
			lom.BadCksum = true
			err = lom.BadCksumErr(cksumFromFS)
			lom.Uncache()
		}
	}
	return err
}

// ValidateDiskChecksum validates if checksum stored in lom's in-memory metadata
// matches object's content checksum.
// Use lom.ValidateMetaChecksum() to check lom's checksum vs on-disk metadata.
func (lom *LOM) ValidateContentChecksum() (err error) {
	var (
		storedCksum, computedCksum string
		cksumType                  = lom.CksumConf().Type
	)
	if cksumType == cmn.ChecksumNone {
		return
	}
	cmn.Assert(cksumType == cmn.ChecksumXXHash) // sha256 et al. not implemented yet

	if lom.md.cksum != nil {
		_, storedCksum = lom.md.cksum.Get()
	}
	// compute
	if computedCksum, err = lom.computeXXHash(lom.FQN, lom.md.size); err != nil {
		return
	}
	if storedCksum == "" { // store with lom meta on disk
		oldCksm := lom.md.cksum
		lom.md.cksum = cmn.NewCksum(cksumType, computedCksum)
		if err := lom.Persist(); err != nil {
			lom.md.cksum = oldCksm
			return err
		}
		lom.ReCache()
		return
	}
	v := cmn.NewCksum(cksumType, computedCksum)
	if cmn.EqCksum(lom.md.cksum, v) {
		return
	}
	// reload meta and check again
	if _, err = lom.lmfs(true); err == nil {
		if cmn.EqCksum(lom.md.cksum, v) {
			return
		}
	}
	lom.BadCksum = true
	err = lom.BadCksumErr(v)
	lom.Uncache()
	return
}

func (lom *LOM) CksumComputeIfMissing() (cksum *cmn.Cksum, err error) {
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
	val, err = lom.computeXXHash(lom.FQN, lom.md.size)
	if err != nil {
		return
	}
	cksum = cmn.NewCksum(cmn.ChecksumXXHash, val)
	return
}

func (lom *LOM) computeXXHash(fqn string, size int64) (cksumValue string, err error) {
	file, err := os.Open(fqn)
	if err != nil {
		err = fmt.Errorf("%s, err: %v", fqn, err)
		return
	}
	buf, slab := lom.T.GetMem2().AllocForSize(size)
	cksumValue, err = cmn.ComputeXXHash(file, buf)
	file.Close()
	slab.Free(buf)
	return
}

// NOTE: Clone performs shallow copy of the LOM struct.
func (lom *LOM) Clone(fqn string) *LOM {
	dst := &LOM{}
	*dst = *lom
	dst.md = lom.md
	if fqn == lom.FQN {
		dst.ParsedFQN = lom.ParsedFQN
	} else {
		dst.FQN = fqn
	}
	return dst
}

// Local Object Metadata (LOM) - is cached. Respectively, lifecycle of any given LOM
// instance includes the following steps:
// 1) construct LOM instance and initialize its runtime state: lom = LOM{...}.Init()
// 2) load persistent state (aka lmeta) from one of the LOM caches or the underlying
//    filesystem: lom.Load(); Load(false) also entails *not adding* LOM to caches
//    (useful when deleting or moving objects
// 3) use: via lom.Atime(), lom.Cksum(), lom.Exists() and numerous other accessors
//    It is illegal to check LOM's existence and, generally, do almost anything
//    with it prior to loading - see previous
// 4) update persistent state in memory: lom.Set*() methods
//    (requires subsequent re-caching via lom.ReCache())
// 5) update persistent state on disk: lom.Persist()
// 6) remove a given LOM instance from cache: lom.Uncache()
// 7) evict an entire bucket-load of LOM cache: cluster.EvictCache(bucket)
// 8) periodic (lazy) eviction followed by access-time synchronization: see LomCacheRunner
// =======================================================================================
func (lom *LOM) Hkey() (string, int) {
	cmn.Dassert(lom.ParsedFQN.Digest != 0, pkgName)
	return lom.md.uname, int(lom.ParsedFQN.Digest & fs.LomCacheMask)
}
func (lom *LOM) Init(bucket, provider string, config ...*cmn.Config) (err error) {
	if lom.FQN != "" {
		cmn.Dassert(bucket == "", pkgName) // either/or
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN)
		if err != nil {
			return
		}
		bucket = lom.ParsedFQN.Bucket
		lom.Objname = lom.ParsedFQN.ObjName
		prov := lom.ParsedFQN.Provider
		if provider == "" {
			provider = prov
		} else if prov1, _ := cmn.ProviderFromStr(provider); prov1 != prov {
			err = fmt.Errorf("unexpected provider %s for FQN [%s]", provider, lom.FQN)
			return
		}
	}
	bowner := lom.T.GetBowner()
	lom.bck.Name, lom.bck.Provider = bucket, provider
	var b = &lom.bck
	if err = b.Init(bowner); err != nil {
		return
	}
	if lom.FQN == "" {
		lom.ParsedFQN.MpathInfo, lom.ParsedFQN.Digest, err = hrwMpath(bucket, lom.Objname)
		if err != nil {
			return
		}
		lom.ParsedFQN.ContentType = fs.ObjectType
		lom.FQN = fs.CSM.FQN(lom.ParsedFQN.MpathInfo, fs.ObjectType, bucket, lom.bck.Provider, lom.Objname)
		lom.HrwFQN = lom.FQN
		lom.ParsedFQN.Provider = lom.bck.Provider
		lom.ParsedFQN.Bucket, lom.ParsedFQN.ObjName = bucket, lom.Objname
	}
	lom.md.uname = Bo2Uname(bucket, lom.Objname)
	if len(config) > 0 {
		lom.config = config[0]
	}
	return
}
func (lom *LOM) IsLoaded() (ok bool) {
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
	)
	_, ok = cache.M.Load(hkey)
	return
}

func (lom *LOM) Load(adds ...bool) (err error) {
	// fast path
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
		add       = true // default: cache it
	)
	if len(adds) > 0 {
		add = adds[0]
	}
	lom.loaded = true
	if lom.FQN == lom.HrwFQN {
		if md, ok := cache.M.Load(hkey); ok {
			lom.exists = true
			lmeta := md.(*lmeta)
			lom.md = *lmeta
			if lom.Exists() {
				return
			}
		}
	} else {
		add = false
	}
	// slow path
	err = lom.FromFS()
	if err == nil && lom.exists {
		lom.md.bckID = lom.Bprops().BID
		if !add {
			return
		}
		md := &lmeta{}
		*md = lom.md
		cache.M.Store(hkey, md)
	}
	return
}

// this code does an extra check only for an ais bucket
func (lom *LOM) Exists() bool {
	cmn.Dassert(lom.loaded, pkgName) // cannot check existence without first calling lom.Load()
	if lom.IsAIS() && lom.exists {
		bmd := lom.T.GetBowner().Get()
		lom.bck.Props, _ = bmd.Get(lom.Bucket(), lom.IsAIS())
		if lom.bck.Props != nil && lom.md.bckID == lom.bck.Props.BID {
			return true
		}
		// glog.Errorf("%s: md.BID %x != %x bprops.BID", lom, lom.md.bckID, lom.BckProps.BID) TODO -- FIXME vs copylb | renamelb
		lom.Uncache()
		lom.exists = false
		return false
	}
	// not yet enforcing Cloud buckets pre-(PUT/cold-GET)-existence
	return lom.exists
}

func (lom *LOM) ReCache() {
	cmn.Dassert(!lom.IsCopy(), pkgName) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
		md        = &lmeta{}
	)
	*md = lom.md
	md.bckID = lom.Bprops().BID
	cache.M.Store(hkey, md)
	lom.loaded = true
}

func (lom *LOM) Uncache() {
	cmn.Dassert(!lom.IsCopy(), pkgName) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
	)
	cache.M.Delete(hkey)
}

func (lom *LOM) FromFS() error {
	lom.exists = true
	if _, err := lom.lmfs(true); err != nil {
		lom.exists = false
		if os.IsNotExist(err) {
			return nil
		}
		if err == syscall.ENODATA || err == syscall.ENOENT {
			return nil
		}
		if _, errex := os.Stat(lom.FQN); os.IsNotExist(errex) {
			return nil
		}
		err = fmt.Errorf("%s: errmeta %v, %T", lom.StringEx(), err, err)
		lom.T.FSHC(err, lom.FQN)
		return err
	}
	// fstat & atime
	finfo, err1 := os.Stat(lom.FQN)
	if err1 != nil {
		lom.exists = false
		if !os.IsNotExist(err1) {
			err := fmt.Errorf("%s: errstat %v", lom.StringEx(), err1)
			lom.T.FSHC(err, lom.FQN)
			return err
		}
		return nil
	}
	if lom.md.size != finfo.Size() { // corruption or tampering
		return fmt.Errorf("%s: errsize (%d != %d)", lom.StringEx(), lom.md.size, finfo.Size())
	}
	atime := ios.GetATime(finfo)
	lom.md.atime = atime.UnixNano()
	lom.md.atimefs = lom.md.atime
	return nil
}

func (lom *LOM) Remove() (err error) {
	lom.Uncache()
	if err = lom.DelAllCopies(); err != nil {
		glog.Errorf("%s: %s", lom, err)
	}
	err = os.Remove(lom.FQN)
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return
}

//
// evict lom cache
//
func EvictCache(bucket string) {
	var (
		caches = lomCaches()
		wg     = &sync.WaitGroup{}
	)
	for _, cache := range caches {
		wg.Add(1)
		go func(cache *fs.LomCache) {
			fevict := func(hkey, _ interface{}) bool {
				uname := hkey.(string)
				b, _ := Uname2Bo(uname)
				if bucket == b {
					cache.M.Delete(hkey)
				}
				return true
			}
			cache.M.Range(fevict)
			wg.Done()
		}(cache)
	}
	wg.Wait()
}

//
// lom cache runner
//
const (
	oomEvictAtime = time.Minute      // OOM
	oomTimeIntval = time.Second * 10 // ===/===
	mpeEvictAtime = time.Minute * 5  // extreme
	mpeTimeIntval = time.Minute      // ===/===
	mphEvictAtime = time.Minute * 10 // high
	mphTimeIntval = time.Minute * 2  // ===/===
	mpnEvictAtime = time.Hour        // normal
	mpnTimeIntval = time.Minute * 10 // ===/===
	minSize2Evict = cmn.KiB * 256
)

var (
	minEvict = int(minSize2Evict / unsafe.Sizeof(lmeta{}))
)

func lomCacheCleanup(t Target, d time.Duration) (evictedCnt, totalCnt int) {
	var (
		caches         = lomCaches()
		now            = time.Now()
		bmd            = t.GetBowner().Get()
		wg             = &sync.WaitGroup{}
		evicted, total atomic.Uint32
	)

	for _, cache := range caches {
		wg.Add(1)
		go func(cache *fs.LomCache) {
			feviat := func(hkey, value interface{}) bool {
				var (
					md    = value.(*lmeta)
					atime = time.Unix(0, md.atime)
				)
				total.Add(1)
				if now.Sub(atime) < d {
					return true
				}
				if md.atime != md.atimefs {
					if lom, err := lomFromLmeta(md, bmd); err == nil {
						lom.flushAtime(atime)
					}
					// TODO: throttle via mountpath.IsIdle(), etc.
				}
				cache.M.Delete(hkey)
				evicted.Add(1)
				return true
			}
			cache.M.Range(feviat)
			wg.Done()
		}(cache)
	}
	wg.Wait()
	return int(evicted.Load()), int(total.Load())

}

func LomCacheHousekeep(mem *memsys.Mem2, t Target) (housekeep hk.CleanupFunc, initialInterval time.Duration) {
	initialInterval = 10 * time.Minute
	housekeep = func() (d time.Duration) {
		switch p := mem.MemPressure(); p { // TODO: heap-memory-arbiter (HMA) abstraction TBD
		case memsys.OOM:
			d = oomEvictAtime
		case memsys.MemPressureExtreme:
			d = mpeEvictAtime
		case memsys.MemPressureHigh:
			d = mphEvictAtime
		default:
			d = mpnEvictAtime
		}

		evicted, total := lomCacheCleanup(t, d)
		if evicted < minEvict {
			d = mpnTimeIntval
		} else {
			switch p := mem.MemPressure(); p {
			case memsys.OOM:
				d = oomTimeIntval
			case memsys.MemPressureExtreme:
				d = mpeTimeIntval
			case memsys.MemPressureHigh:
				d = mphTimeIntval
			default:
				d = mpnTimeIntval
			}
		}
		cmn.Assert(total >= evicted)
		glog.Infof("total %d, evicted %d, timer %v", total-evicted, evicted, d)
		return
	}
	return
}

func (lom *LOM) flushAtime(atime time.Time) {
	finfo, err := os.Stat(lom.FQN)
	if err != nil {
		return
	}
	mtime := finfo.ModTime()
	if err = os.Chtimes(lom.FQN, atime, mtime); err != nil {
		glog.Errorf("%s: flush atime err: %v", lom, err)
	}
}

//
// static helpers
//
func lomFromLmeta(md *lmeta, bmd *BMD) (lom *LOM, err error) {
	var (
		bucket, objName = Uname2Bo(md.uname)
		local, exists   bool
	)
	lom = &LOM{Objname: objName}
	lom.bck.Name = bucket
	if bmd.Exists(bucket, md.bckID, true /*local*/) {
		local, exists = true, true
	} else if bmd.Exists(bucket, md.bckID, false /*cloud*/) {
		local, exists = false, true
	}
	lom.exists = exists
	if exists {
		lom.bck.Provider = cmn.ProviderFromBool(local)
		lom.FQN, _, err = HrwFQN(fs.ObjectType, lom.Bck(), lom.Objname)
	}
	return
}

func lomCaches() []*fs.LomCache {
	availablePaths, _ := fs.Mountpaths.Get()
	var (
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
	return caches
}

//
// lock/unlock
//

func getLomLocker(idx int) *nlc { return &lomLocker[idx] }

func (lom *LOM) TryLock(exclusive bool) bool {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	return nlc.TryLock(lom.Uname(), exclusive)
}
func (lom *LOM) Lock(exclusive bool) {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	cmn.Assert(lom.Uname() != "") // TODO: remove
	nlc.Lock(lom.Uname(), exclusive)
}
func (lom *LOM) DowngradeLock() {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	nlc.DowngradeLock(lom.Uname())
}
func (lom *LOM) Unlock(exclusive bool) {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	nlc.Unlock(lom.Uname(), exclusive)
}
