// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/transport"
)

// Local Object Metadata (LOM) is a locally stored object metadata comprising, in part:
// - name, version, atime, checksum, size, etc. object attributes and flags
// - runtime context including properties and configuration of the bucket
//   that contains this LOM

const (
	fmtNestedErr      = "nested err: %v"
	lomInitialVersion = "1"
	lomDirtyMask      = uint64(1 << 63)
)

type (
	lmeta struct {
		copies fs.MPI
		uname  string
		cmn.ObjAttrs
		atimefs uint64 // NOTE: high bit is reserved for `dirty`
		bckID   uint64
	}
	LOM struct {
		bck         Bck
		ObjName     string
		mpathInfo   *fs.MountpathInfo
		FQN         string
		HrwFQN      string // (=> main replica)
		info        string
		md          lmeta  // on-disk metadata
		mpathDigest uint64 // mountpath's digest
	}
)

var (
	lomLocker nameLocker
	maxLmeta  atomic.Int64
	T         Target
)

// interface guard
var (
	_ cmn.ObjAttrsHolder = (*LOM)(nil)
	_ fs.PartsFQN        = (*LOM)(nil)
	_ lifUnlocker        = (*LOM)(nil)
)

func Init(t Target) {
	initBckLocker()
	if t == nil { // am proxy
		return
	}
	initLomLocker()
	T = t
}

func initLomLocker() {
	lomLocker = make(nameLocker, cos.MultiSyncMapCount)
	lomLocker.init()
	maxLmeta.Store(xattrMaxSize)
}

/////////
// LOM //
/////////

func (lom *LOM) ObjAttrs() *cmn.ObjAttrs { return &lom.md.ObjAttrs }

// LOM == remote-object equality check
func (lom *LOM) Equal(rem cmn.ObjAttrsHolder) (equal bool) { return lom.ObjAttrs().Equal(rem) }

func (lom *LOM) CopyAttrs(oah cmn.ObjAttrsHolder, skipCksum bool) {
	lom.md.ObjAttrs.CopyFrom(oah, skipCksum)
}

// special a) when a new version is being created b) for usage in unit tests
func (lom *LOM) SizeBytes(special ...bool) int64 {
	debug.AssertMsg(len(special) > 0 || lom.loaded(), lom.String())
	return lom.md.Size
}

func (lom *LOM) Version(special ...bool) string {
	debug.Assert(len(special) > 0 || lom.loaded())
	return lom.md.Ver
}

func (lom *LOM) Uname() string { return lom.md.uname }

func (lom *LOM) SetSize(size int64)    { lom.md.Size = size }
func (lom *LOM) SetVersion(ver string) { lom.md.Ver = ver }

func (lom *LOM) Checksum() *cos.Cksum          { return lom.md.Cksum }
func (lom *LOM) SetCksum(cksum *cos.Cksum)     { lom.md.Cksum = cksum }
func (lom *LOM) EqCksum(cksum *cos.Cksum) bool { return lom.md.Cksum.Equal(cksum) }

func (lom *LOM) Atime() time.Time      { return time.Unix(0, lom.md.Atime) }
func (lom *LOM) AtimeUnix() int64      { return lom.md.Atime }
func (lom *LOM) SetAtimeUnix(tu int64) { lom.md.Atime = tu }

// 946771140000000000 = time.Parse(time.RFC3339Nano, "2000-01-01T23:59:00Z").UnixNano()
// and note that prefetch sets atime=-now
func isValidAtime(atime int64) bool {
	return atime < -946771140000000000 || atime > 946771140000000000
}

// custom metadata
func (lom *LOM) GetCustomMD() cos.SimpleKVs   { return lom.md.GetCustomMD() }
func (lom *LOM) SetCustomMD(md cos.SimpleKVs) { lom.md.SetCustomMD(md) }

func (lom *LOM) GetCustomKey(key string) (string, bool) { return lom.md.GetCustomKey(key) }
func (lom *LOM) SetCustomKey(key, value string)         { lom.md.SetCustomKey(key, value) }

// lom <= transport.ObjHdr (NOTE: caller must call freeLOM)
func AllocLomFromHdr(hdr *transport.ObjHdr) (lom *LOM, err error) {
	lom = AllocLOM(hdr.ObjName)
	if err = lom.InitBck(&hdr.Bck); err != nil {
		return
	}
	lom.CopyAttrs(&hdr.ObjAttrs, false /*skip checksum*/)
	return
}

func (lom *LOM) ECEnabled() bool { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsHRW() bool     { return lom.HrwFQN == lom.FQN } // subj to resilvering

func (lom *LOM) Bprops() *cmn.BucketProps { return lom.bck.Props }

func (lom *LOM) MirrorConf() *cmn.MirrorConf  { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf    { return lom.bck.CksumConf() }
func (lom *LOM) CksumType() string            { return lom.bck.CksumConf().Type }
func (lom *LOM) VersionConf() cmn.VersionConf { return lom.bck.VersionConf() }

// as fs.PartsFQN
func (lom *LOM) ObjectName() string           { return lom.ObjName }
func (lom *LOM) Bck() *Bck                    { return &lom.bck }
func (lom *LOM) Bucket() *cmn.Bck             { return (*cmn.Bck)(&lom.bck) }
func (lom *LOM) MpathInfo() *fs.MountpathInfo { return lom.mpathInfo }

// see also: transport.ObjHdr.FullName()
func (lom *LOM) FullName() string { return filepath.Join(lom.bck.Name, lom.ObjName) }

func (lom *LOM) WritePolicy() (p apc.WritePolicy) {
	if bprops := lom.Bprops(); bprops == nil {
		p = apc.WriteImmediate
	} else {
		p = bprops.WritePolicy.MD
	}
	return
}

func (lom *LOM) loaded() bool { return lom.md.bckID != 0 }

func (lom *LOM) HrwTarget(smap *Smap) (tsi *Snode, local bool, err error) {
	tsi, err = HrwTarget(lom.Uname(), smap)
	if err != nil {
		return
	}
	local = tsi.ID() == T.SID()
	return
}

func (lom *LOM) IncVersion() error {
	debug.Assert(lom.Bck().IsAIS())
	if lom.md.Ver == "" {
		lom.SetVersion(lomInitialVersion)
		return nil
	}
	ver, err := strconv.Atoi(lom.md.Ver)
	if err != nil {
		return fmt.Errorf("%s: %v", lom, err)
	}
	lom.SetVersion(strconv.Itoa(ver + 1))
	return nil
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
		md  *lmeta
		err error
	)
	if lom.CksumType() == cos.ChecksumNone {
		return nil
	}
	wmd := lom.WritePolicy()
	if wmd == apc.WriteNever || (wmd == apc.WriteDelayed && lom.md.isDirty()) {
		// cannot validate meta checksum
		return nil
	}
	md, err = lom.lmfsReload(false)
	if err != nil {
		return err
	}
	if md == nil {
		return fmt.Errorf("%s: no meta", lom)
	}
	if lom.md.Cksum == nil {
		lom.SetCksum(md.Cksum)
		return nil
	}
	// different versions may have different checksums
	if md.Ver == lom.md.Ver && !lom.EqCksum(md.Cksum) {
		err = cos.NewBadDataCksumError(lom.md.Cksum, md.Cksum, lom.String())
		lom.Uncache(true /*delDirty*/)
	}
	return err
}

// ValidateDiskChecksum validates if checksum stored in lom's in-memory metadata
// matches object's content checksum.
// Use lom.ValidateMetaChecksum() to check lom's checksum vs on-disk metadata.
func (lom *LOM) ValidateContentChecksum() (err error) {
	var (
		cksumType = lom.CksumType()
		cksums    = struct {
			stor *cos.Cksum     // stored with LOM
			comp *cos.CksumHash // computed
		}{stor: lom.md.Cksum}
		reloaded bool
	)
recomp:
	if cksumType == cos.ChecksumNone { // as far as do-no-checksum-checking bucket rules
		return
	}
	if !lom.md.Cksum.IsEmpty() {
		cksumType = lom.md.Cksum.Ty() // takes precedence on the other hand
	}
	if cksums.comp, err = lom.ComputeCksum(cksumType); err != nil {
		return
	}
	if lom.md.Cksum.IsEmpty() { // store computed
		lom.md.Cksum = cksums.comp.Clone()
		if !lom.loaded() {
			lom.SetAtimeUnix(time.Now().UnixNano())
		}
		if err = lom.Persist(); err != nil {
			lom.md.Cksum = cksums.stor
		}
		return
	}
	if cksums.comp.Equal(lom.md.Cksum) {
		return
	}
	if reloaded {
		goto ex
	}
	// retry: load from disk and check again
	reloaded = true
	if _, err = lom.lmfsReload(true); err == nil && lom.md.Cksum != nil {
		if cksumType == lom.md.Cksum.Ty() {
			if cksums.comp.Equal(lom.md.Cksum) {
				return
			}
		} else { // type changed
			cksums.stor = lom.md.Cksum
			cksumType = lom.CksumType()
			goto recomp
		}
	}
ex:
	err = cos.NewBadDataCksumError(&cksums.comp.Cksum, cksums.stor, lom.String())
	lom.Uncache(true /*delDirty*/)
	return
}

func (lom *LOM) ComputeSetCksum() (*cos.Cksum, error) {
	var (
		cksum          *cos.Cksum
		cksumHash, err = lom.ComputeCksum(lom.CksumType())
	)
	if err != nil {
		return nil, err
	}
	if cksumHash != nil {
		cksum = cksumHash.Clone()
	}
	lom.SetCksum(cksum)
	return cksum, nil
}

func (lom *LOM) ComputeCksum(cksumType string) (cksum *cos.CksumHash, err error) {
	var file *os.File
	if cksumType == cos.ChecksumNone {
		return
	}
	if file, err = os.Open(lom.FQN); err != nil {
		return
	}
	// No need to allocate `buf` as `io.Discard` has efficient `io.ReaderFrom` implementation.
	_, cksum, err = cos.CopyAndChecksum(io.Discard, file, nil, cksumType)
	cos.Close(file)
	if err != nil {
		return nil, err
	}
	return
}

//   - locked: is locked by the immediate caller (or otherwise is known to be locked);
//     if false, try Rlock temporarily *if and only when* reading from FS
func (lom *LOM) Load(cacheit, locked bool) (err error) {
	var (
		lcache, lmd = lom.fromCache()
		bmd         = T.Bowner().Get()
	)
	// fast path
	if lmd != nil {
		lom.md = *lmd
		err = lom._checkBucket(bmd)
		return
	}
	// slow path
	if !locked && lom.TryLock(false) {
		defer lom.Unlock(false)
	}
	err = lom.FromFS()
	if err != nil {
		return
	}
	bid := lom.Bprops().BID
	debug.AssertMsg(bid != 0, lom.FullName())
	if bid == 0 {
		return
	}
	lom.md.bckID = bid
	err = lom._checkBucket(bmd)
	if err != nil {
		return
	}
	if cacheit && lcache != nil {
		md := lom.md
		lcache.Store(lom.md.uname, &md)
	}
	return
}

func (lom *LOM) _checkBucket(bmd *BMD) (err error) {
	err = bmd.eqBID(&lom.bck, lom.md.bckID)
	if err == errBucketIDMismatch {
		err = cmn.NewErrObjDefunct(lom.String(), lom.md.bckID, lom.bck.Props.BID)
	}
	return
}

//
// lom cache
//

// store new or refresh existing
func (lom *LOM) Recache() {
	debug.Assert(!lom.IsCopy())
	md := lom.md
	bid := lom.Bprops().BID
	debug.Assert(bid != 0)

	lcache, lmd := lom.fromCache()
	if lmd != nil {
		md.cpAtime(lmd)
	}
	md.bckID, lom.md.bckID = bid, bid
	lcache.Store(lom.md.uname, &md)
}

func (lom *LOM) Uncache(delDirty bool) {
	if delDirty {
		lcache := lom.lcache()
		if md, ok := lcache.LoadAndDelete(lom.md.uname); ok {
			lmd := md.(*lmeta)
			lom.md.cpAtime(lmd)
		}
		return
	}

	lcache, lmd := lom.fromCache()
	if lmd == nil {
		return
	}
	if !lmd.isDirty() {
		lom.md.cpAtime(lmd)
		lcache.Delete(lom.md.uname)
	}
}

func (lom *LOM) CacheIdx() int     { return fs.LcacheIdx(lom.mpathDigest) }
func (lom *LOM) lcache() *sync.Map { return lom.mpathInfo.LomCache(lom.CacheIdx()) }

func (lom *LOM) fromCache() (lcache *sync.Map, lmd *lmeta) {
	lcache = lom.lcache()
	if md, ok := lcache.Load(lom.md.uname); ok {
		lmd = md.(*lmeta)
	}
	return
}

func (lom *LOM) FromFS() error {
	finfo, atimefs, err := ios.FinfoAtime(lom.FQN)
	if err != nil {
		if !os.IsNotExist(err) {
			err = os.NewSyscallError("stat", err)
			T.FSHC(err, lom.FQN)
		}
		return err
	}
	if _, err = lom.lmfs(true); err != nil {
		// retry once
		if cmn.IsErrLmetaNotFound(err) {
			runtime.Gosched()
			_, err = lom.lmfs(true)
		}
	}
	if err != nil {
		if !cmn.IsErrLmetaNotFound(err) {
			T.FSHC(err, lom.FQN)
		}
		return err
	}
	// fstat & atime
	if lom.md.Size != finfo.Size() { // corruption or tampering
		return cmn.NewErrLmetaCorrupted(lom.whingeSize(finfo.Size()))
	}
	lom.md.Atime = atimefs
	lom.md.atimefs = uint64(atimefs)
	return nil
}

func (lom *LOM) whingeSize(size int64) error {
	return fmt.Errorf("errsize (%d != %d)", lom.md.Size, size)
}

func (lom *LOM) Remove(force ...bool) (err error) {
	// making "rlock" exception to be able to (forcefully) remove corrupted obj in the GET path
	debug.AssertFunc(func() bool {
		rc, exclusive := lom.IsLocked()
		return exclusive || (len(force) > 0 && force[0] && rc > 0)
	})
	lom.Uncache(true /*delDirty*/)
	err = cos.RemoveFile(lom.FQN)
	if os.IsNotExist(err) {
		err = nil
	}
	for copyFQN := range lom.md.copies {
		if erc := cos.RemoveFile(copyFQN); erc != nil && !os.IsNotExist(erc) {
			err = erc
		}
	}
	lom.md.bckID = 0
	return
}

//
// evict lom cache
//

func EvictLomCache(b *Bck) {
	var (
		caches = lomCaches()
		wg     = &sync.WaitGroup{}
	)
	for _, lcache := range caches {
		wg.Add(1)
		go func(cache *sync.Map) {
			cache.Range(func(hkey, _ any) bool {
				uname := hkey.(string)
				bck, _ := cmn.ParseUname(uname)
				if bck.Equal((*cmn.Bck)(b)) {
					cache.Delete(hkey)
				}
				return true
			})
			wg.Done()
		}(lcache)
	}
	wg.Wait()
}

func lomCaches() []*sync.Map {
	var (
		i              int
		availablePaths = fs.GetAvail()
		cachesCnt      = len(availablePaths) * cos.MultiSyncMapCount
		caches         = make([]*sync.Map, cachesCnt)
	)
	for _, mi := range availablePaths {
		for idx := 0; idx < cos.MultiSyncMapCount; idx++ {
			caches[i] = mi.LomCache(idx)
			i++
		}
	}
	return caches
}

//
// lock/unlock
//

func (lom *LOM) getLocker() *nlc { return &lomLocker[lom.CacheIdx()] }

func (lom *LOM) IsLocked() (int /*rc*/, bool /*exclusive*/) {
	nlc := lom.getLocker()
	return nlc.IsLocked(lom.Uname())
}

func (lom *LOM) TryLock(exclusive bool) bool {
	nlc := lom.getLocker()
	return nlc.TryLock(lom.Uname(), exclusive)
}

func (lom *LOM) Lock(exclusive bool) {
	nlc := lom.getLocker()
	nlc.Lock(lom.Uname(), exclusive)
}

func (lom *LOM) UpgradeLock() (finished bool) {
	nlc := lom.getLocker()
	return nlc.UpgradeLock(lom.Uname())
}

func (lom *LOM) DowngradeLock() {
	nlc := lom.getLocker()
	nlc.DowngradeLock(lom.Uname())
}

func (lom *LOM) Unlock(exclusive bool) {
	nlc := lom.getLocker()
	nlc.Unlock(lom.Uname(), exclusive)
}

// (compare with cos.CreateFile)
func (lom *LOM) CreateFile(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err == nil || !os.IsNotExist(err) {
		return
	}
	// slow path
	bdir := lom.mpathInfo.MakePathBck(lom.Bucket())
	if err = cos.Stat(bdir); err != nil {
		return nil, fmt.Errorf("%s(bdir %s): %w", lom, bdir, err)
	}
	fdir := filepath.Dir(fqn)
	if err = cos.CreateDir(fdir); err != nil {
		return
	}
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	return
}

// (compare with cos.Rename)
func (lom *LOM) RenameFile(workfqn string) error {
	bdir := lom.mpathInfo.MakePathBck(lom.Bucket())
	if err := cos.Stat(bdir); err != nil {
		return fmt.Errorf("%s(bdir %s): %w", lom, bdir, err)
	}
	if err := cos.Rename(workfqn, lom.FQN); err != nil {
		return cmn.NewErrFailedTo(T, "rename", lom, err)
	}
	return nil
}

// permission to overwrite objects that were previously read from:
// a) any remote backend that is currently not configured as the bucket's backend
// b) HTPP ("ht://") since it's not writable
func (lom *LOM) AllowDisconnectedBackend(loaded bool) (err error) {
	bck := lom.Bck()
	// allowed
	if bck.Props.Access.Has(apc.AceDisconnectedBackend) {
		return
	}
	if !loaded {
		// doesn't exist
		if lom.Load(true /*cache it*/, false /*locked*/) != nil {
			return
		}
	}
	// not allowed & exists & no remote source
	srcProvider, hasSrc := lom.GetCustomKey(cmn.SourceObjMD)
	if !hasSrc {
		return
	}
	// case 1
	if bck.IsAIS() {
		goto rerr
	}
	// case 2
	if b := bck.RemoteBck(); b != nil && b.Provider == srcProvider {
		return
	}
rerr:
	msg := fmt.Sprintf("%s(downoaded from %q)", lom, srcProvider)
	err = cmn.NewObjectAccessDenied(msg, apc.AccessOp(apc.AceDisconnectedBackend), bck.Props.Access)
	return
}
