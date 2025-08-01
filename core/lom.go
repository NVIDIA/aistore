// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/memsys"
)

// Local Object Metadata (LOM) is a locally stored object metadata comprising, in part:
// - name, version, atime, checksum, size, etc. object attributes and flags
// - runtime context including properties and configuration of the bucket
//   that contains this LOM

const (
	lomInitialVersion = "1"
)

// core stats
const (
	RemoteDeletedDelCount = "remote.deleted.del.n"

	// lcache stats
	LcacheCollisionCount = "lcache.collision.n"
	LcacheEvictedCount   = "lcache.evicted.n"
	LcacheErrCount       = "err.lcache.n" // errPrefix + "lcache.n"
	LcacheFlushColdCount = "lcache.flush.cold.n"
)

type (
	lmeta struct { // sizeof = 72
		copies fs.MPI
		uname  *string
		cmn.ObjAttrs
		atimefs uint64 // (high bit `lomDirtyMask` | int64: atime)
		lid     lomBID // (for bitwise structure, see lombid.go)
	}
	LOM struct {
		mi      *fs.Mountpath
		bck     meta.Bck
		ObjName string
		FQN     string
		HrwFQN  *string // (=> main replica)
		md      lmeta   // on-disk metadata
		digest  uint64  // uname digest
	}
)

type (
	global struct {
		pmm      *memsys.MMSA
		smm      *memsys.MMSA
		locker   nameLocker
		lchk     lchk
		maxLmeta atomic.Int64
	}
)

var bckLocker nameLocker // common

// target only
var (
	T Target
	g global

	// pack/unpack internals
	recdupSepa [lenRecSepa]byte
)

// interface guard
var (
	_ cos.OAH     = (*LOM)(nil)
	_ fs.PartsFQN = (*LOM)(nil)
	_ lifUnlocker = (*LOM)(nil)
)

func Pinit() { bckLocker = newNameLocker() }

func Tinit(t Target, config *cmn.Config, runHK bool) {
	bckLocker = newNameLocker()
	T = t
	{
		g.maxLmeta.Store(xattrLomSize)
		g.locker = newNameLocker()
		g.pmm = t.PageMM()
		g.smm = t.ByteMM()
	}
	if runHK {
		g.lchk.init(config)
	}
	for i := range recordSepa {
		recdupSepa[i] = recordSepa[i]
	}
}

func Term() {
	const sleep = time.Second >> 2 // total <= 2s
	for i := 0; i < 8 && !g.lchk.running.CAS(false, true); i++ {
		time.Sleep(sleep)
	}
	g.lchk.term()
}

/////////
// LOM //
/////////

func (lom *LOM) ObjAttrs() *cmn.ObjAttrs { return &lom.md.ObjAttrs }

// LOM == remote-object equality check
func (lom *LOM) CheckEq(rem cos.OAH) error { return lom.ObjAttrs().CheckEq(rem) }

func (lom *LOM) CopyAttrs(oah cos.OAH, skipCksum bool) {
	lom.md.ObjAttrs.CopyFrom(oah, skipCksum)
}

// special a) when a new version is being created b) for usage in unit tests
func (lom *LOM) Lsize(special ...bool) int64 {
	debug.Assert(len(special) > 0 || lom.loaded(), lom.String())
	return lom.md.Size
}

// low-level access to the os.FileInfo of a chunk or whole file
func (lom *LOM) Fstat(getAtime bool) (size, atimefs int64, mtime time.Time, _ error) {
	finfo, err := os.Stat(lom.FQN)
	if err == nil {
		size = finfo.Size() // NOTE: chunk?
		mtime = finfo.ModTime()
		if getAtime {
			atimefs = ios.GetATime(finfo).UnixNano()
		}
	}
	return size, atimefs, mtime, err
}

func (lom *LOM) Version(special ...bool) string {
	debug.Assert(len(special) > 0 || lom.loaded())
	return lom.md.Version()
}

func (lom *LOM) VersionPtr() *string     { return lom.md.Ver }
func (lom *LOM) SetVersion(ver string)   { lom.md.SetVersion(ver) }
func (lom *LOM) CopyVersion(oah cos.OAH) { lom.md.CopyVersion(oah) }

func (lom *LOM) Uname() string     { return *lom.md.uname }
func (lom *LOM) UnamePtr() *string { return lom.md.uname }
func (lom *LOM) Digest() uint64    { return lom.digest }

func (lom *LOM) SetSize(size int64) { lom.md.Size = size }

func (lom *LOM) Checksum() *cos.Cksum      { return lom.md.Cksum }
func (lom *LOM) SetCksum(cksum *cos.Cksum) { lom.md.Cksum = cksum }
func (lom *LOM) EqCksum(cksum *cos.Cksum) bool {
	return !lom.md.Cksum.IsEmpty() && lom.md.Cksum.Equal(cksum)
}

func (lom *LOM) Atime() time.Time      { return time.Unix(0, lom.md.Atime) }
func (lom *LOM) AtimeUnix() int64      { return lom.md.Atime }
func (lom *LOM) SetAtimeUnix(tu int64) { lom.md.Atime = tu }

func (lom *LOM) bid() uint64             { return lom.md.lid.bid() }
func (lom *LOM) setbid(bpropsBID uint64) { lom.md.lid = lom.md.lid.setbid(bpropsBID) }

// custom metadata
func (lom *LOM) GetCustomMD() cos.StrKVs   { return lom.md.GetCustomMD() }
func (lom *LOM) SetCustomMD(md cos.StrKVs) { lom.md.SetCustomMD(md) }

func (lom *LOM) GetCustomKey(key string) (string, bool) { return lom.md.GetCustomKey(key) }
func (lom *LOM) SetCustomKey(key, value string)         { lom.md.SetCustomKey(key, value) }

// subj to resilvering
func (lom *LOM) IsHRW() bool {
	p := &lom.FQN
	return lom.HrwFQN == p || lom.FQN == *lom.HrwFQN
}

// assorted accessors
func (lom *LOM) Bck() *meta.Bck                 { return &lom.bck }
func (lom *LOM) Bprops() *cmn.Bprops            { return lom.bck.Props }
func (lom *LOM) ECEnabled() bool                { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsFeatureSet(f feat.Flags) bool { return lom.Bprops().Features.IsSet(f) }
func (lom *LOM) MirrorConf() *cmn.MirrorConf    { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf      { return lom.bck.CksumConf() }
func (lom *LOM) CksumType() string              { return lom.bck.CksumConf().Type }
func (lom *LOM) VersionConf() cmn.VersionConf   { return lom.bck.VersionConf() }
func (lom *LOM) Location() string               { return T.String() + apc.LocationPropSepa + lom.mi.String() }

// as fs.PartsFQN
func (lom *LOM) ObjectName() string       { return lom.ObjName }
func (lom *LOM) Bucket() *cmn.Bck         { return (*cmn.Bck)(&lom.bck) }
func (lom *LOM) Mountpath() *fs.Mountpath { return lom.mi }

// chunks vs whole // TODO: NIY
func (lom *LOM) IsChunked(special ...bool) bool {
	debug.Assert(len(special) > 0 || lom.loaded())
	return false
}

func ParseObjLoc(loc string) (tname, mpname string) {
	i := strings.IndexByte(loc, apc.LocationPropSepa[0])
	tname, mpname = loc[:i], loc[i+1:]
	return
}

// see also: transport.ObjHdr.Cname()
func (lom *LOM) Cname() string { return lom.bck.Cname(lom.ObjName) }

func (lom *LOM) WritePolicy() (p apc.WritePolicy) {
	if bprops := lom.Bprops(); bprops == nil {
		p = apc.WriteImmediate
	} else {
		p = bprops.WritePolicy.MD
	}
	return
}

func (lom *LOM) loaded() bool { return lom.md.lid != 0 }

func (lom *LOM) HrwTarget(smap *meta.Smap) (tsi *meta.Snode, local bool, err error) {
	tsi, err = smap.HrwHash2T(lom.digest)
	if err != nil {
		return
	}
	local = tsi.ID() == T.SID()
	return
}

func (lom *LOM) IncVersion() error {
	debug.Assert(lom.Bck().IsAIS())
	v := lom.md.Version()
	if v == "" {
		lom.SetVersion(lomInitialVersion)
		return nil
	}
	ver, err := strconv.Atoi(v)
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
	if md.Version() == lom.md.Version() && !lom.EqCksum(md.Cksum) {
		err = cos.NewErrDataCksum(lom.md.Cksum, md.Cksum, lom.String())
		lom.UncacheDel()
	}
	return err
}

// ValidateDiskChecksum validates if checksum stored in lom's in-memory metadata
// matches object's content checksum.
// Use lom.ValidateMetaChecksum() to check lom's checksum vs on-disk metadata.
func (lom *LOM) ValidateContentChecksum(locked bool) (err error) {
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
		return nil
	}
	if !lom.md.Cksum.IsEmpty() {
		cksumType = lom.md.Cksum.Ty() // takes precedence on the other hand
	}
	if cksums.comp, err = lom.ComputeCksum(cksumType, locked); err != nil {
		return err
	}
	if lom.md.Cksum.IsEmpty() { // store computed
		lom.md.Cksum = cksums.comp.Clone()
		if !lom.loaded() {
			lom.SetAtimeUnix(time.Now().UnixNano())
		}
		if err = lom.Persist(); err != nil {
			lom.md.Cksum = cksums.stor
		}
		return err
	}
	if cksums.comp.Equal(lom.md.Cksum) {
		return nil
	}
	if reloaded {
		goto ex
	}
	// retry: load from disk and check again
	reloaded = true
	if _, err = lom.lmfsReload(true); err == nil && lom.md.Cksum != nil {
		// type changed - recompute
		if cksumType != lom.md.Cksum.Ty() {
			cksums.stor = lom.md.Cksum
			cksumType = lom.CksumType()
			goto recomp
		}
		// otherwise, check
		if cksums.comp.Equal(lom.md.Cksum) {
			return nil
		}
	}
ex:
	err = cos.NewErrDataCksum(&cksums.comp.Cksum, cksums.stor, lom.String())
	lom.UncacheDel()
	return err
}

func (lom *LOM) ComputeSetCksum(locked bool) (*cos.Cksum, error) {
	var (
		cksum          *cos.Cksum
		cksumHash, err = lom.ComputeCksum(lom.CksumType(), locked)
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

func (lom *LOM) ComputeCksum(cksumType string, locked bool) (cksum *cos.CksumHash, _ error) {
	if cksumType == cos.ChecksumNone {
		return nil, nil
	}
	if !locked {
		lom.Lock(false)
	}
	lmfh, err := lom.Open()
	if err != nil {
		return nil, err
	}
	// No need to allocate `buf` as `io.Discard` has efficient `io.ReaderFrom` implementation.
	_, cksum, err = cos.CopyAndChecksum(io.Discard, lmfh, nil, cksumType)
	cos.Close(lmfh)
	if !locked {
		lom.Unlock(false)
	}
	return cksum, err
}

// no lock is taken when locked by an immediate caller, or otherwise is known to be locked
// otherwise, try Rlock temporarily _if and only when_ reading from fs
//
// (compare w/ LoadUnsafe() below)
func (lom *LOM) Load(cacheit, locked bool) error {
	var (
		lcache, lmd = lom.fromCache()
		bmd         = T.Bowner().Get()
	)
	// fast path
	if lmd != nil {
		lom.md = *lmd
		if lom.IsFntl() {
			lom.fixupFntl()
		}
		return lom._checkBucket(bmd)
	}

	// slow path
	if !locked && lom.TryLock(false) {
		defer lom.Unlock(false)
	}
	if err := lom.FromFS(); err != nil {
		return err
	}

	// MetaverLOM = 1: always zero (not storing lom.md.lid)
	debug.Assert(lom.bid() == 0 || lom.bid() == lom.Bprops().BID, lom.bid())
	lom.setbid(lom.Bprops().BID)

	if err := lom._checkBucket(bmd); err != nil {
		return err
	}
	if cacheit {
		md := lom.md
		lcache.Store(lom.digest, &md)
	}
	return nil
}

func (lom *LOM) _checkBucket(bmd *meta.BMD) error {
	bck := &lom.bck
	bprops, present := bmd.Get(bck)
	if !present {
		return cmn.NewErrBckNotFound(bck.Bucket())
	}
	if lom.bid() != bprops.BID {
		return cmn.NewErrObjDefunct(lom.String(), lom.bid(), bprops.BID)
	}
	return nil
}

// usage: fast (and unsafe) loading object metadata except atime - no locks
// compare with conventional Load() above
func (lom *LOM) LoadUnsafe() (err error) {
	var (
		_, lmd = lom.fromCache()
		bmd    = T.Bowner().Get()
	)
	// fast path
	if lmd != nil {
		lom.md = *lmd
		if lom.IsFntl() {
			lom.fixupFntl()
		}
		return lom._checkBucket(bmd)
	}

	// read and decode xattr; NOTE: fs.GetXattr* vs fs.SetXattr race possible and must be
	// either a) handled or b) benign from the caller's perspective
	if _, err = lom.lmfs(true); err == nil {
		// MetaverLOM = 1: always zero (not storing lom.md.lid)
		debug.Assert(lom.bid() == 0 || lom.bid() == lom.Bprops().BID, lom.bid())
		lom.setbid(lom.Bprops().BID)
		err = lom._checkBucket(bmd)
	}
	return err
}

//
// lom cache -------------------------------------------------------------
//

// store new or refresh existing
func (lom *LOM) Recache() {
	debug.Assert(!lom.IsCopy())
	lom.setbid(lom.Bprops().BID)

	md := lom.md

	lcache := lom.lcache()
	val, ok := lcache.Swap(lom.digest, &md)
	if !ok {
		return
	}
	lmd := val.(*lmeta)
	if *lmd.uname != *lom.md.uname {
		lom._collide(lmd)
	} else {
		// updating the value that's already in the map (race extremely unlikely, benign anyway)
		md.cpAtime(lmd)
	}
}

func (lom *LOM) _collide(lmd *lmeta) {
	if cmn.Rom.FastV(4, cos.SmoduleCore) || lom.digest&0xf == 5 {
		nlog.InfoDepth(1, LcacheCollisionCount, lom.digest, "[", *lmd.uname, "]", *lom.md.uname, lom.Cname())
	}
	T.StatsUpdater().Inc(LcacheCollisionCount)
}

func (lom *LOM) Uncache() {
	lcache := lom.lcache()
	md, ok := lcache.LoadAndDelete(lom.digest)
	if !ok {
		return
	}
	lmd := md.(*lmeta)
	if *lmd.uname != *lom.md.uname {
		lom._collide(lmd)
	} else {
		lom.md.cpAtime(lmd)
	}
}

// upon: error; RemoveObj
func (lom *LOM) UncacheDel() {
	lcache := lom.lcache()
	lcache.Delete(lom.digest)
}

// remove from cache unless dirty
func (lom *LOM) UncacheUnless() {
	lcache, lmd := lom.fromCache()
	if lmd == nil {
		return
	}
	if !lmd.isDirty() {
		lom.md.cpAtime(lmd)
		lcache.Delete(lom.md.uname)
	}
}

func (lom *LOM) CacheIdx() int     { return lcacheIdx(lom.digest) }
func (lom *LOM) lcache() *sync.Map { return lom.mi.LomCaches.Get(lom.CacheIdx()) }

func (lom *LOM) fromCache() (lcache *sync.Map, lmd *lmeta) {
	lcache = lom.lcache()
	if md, ok := lcache.Load(lom.digest); ok {
		lmd = md.(*lmeta)
		if *lmd.uname != *lom.md.uname {
			lom._collide(lmd)
		}
	}
	return
}

func (lom *LOM) FromFS() error {
	size, atimefs, _, err := lom.Fstat(true /*get-atime*/)
	if err == nil {
		goto exist
	}

	switch {
	case cos.IsNotExist(err):
		// instead of *fs.PathError type
		return cos.NewErrNotFound(T, lom.Cname())

	case cos.IsErrNotDir(err) && cos.IsPathErr(err):
		nlog.Warningln(cos.ErrENOTDIR, lom.Cname(), "[", err, "]")
		return cos.NewErrNotFound(T, lom.Cname())

	case cos.IsErrFntl(err):
		lom.md.lid = lomBID(lom.Bprops().BID)
		lom.md.lid = lom.md.lid.setlmfl(lmflFntl)

		// temp substitute to check existence
		short := lom.ShortenFntl()
		saved := lom.PushFntl(short)
		size, atimefs, _, err = lom.Fstat(true)
		if err == nil {
			goto exist
		}

		lom.PopFntl(saved)
		debug.Assert(!cos.IsErrFntl(err))
		if cos.IsNotExist(err) {
			// ditto
			return cos.NewErrNotFound(T, lom.Cname())
		}
		lom.md.lid = lom.md.lid.clrlmfl(lmflFntl)
		fallthrough
	default:
		err = os.NewSyscallError("stat", err)
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	}

exist:
	if _, err = lom.lmfs(true); err != nil {
		// retry once
		if cmn.IsErrLmetaNotFound(err) {
			runtime.Gosched()
			_, err = lom.lmfs(true)
		}
	}
	if err != nil {
		if !cmn.IsErrLmetaNotFound(err) {
			T.FSHC(err, lom.Mountpath(), lom.FQN)
		}
		return err
	}
	// fstat & atime
	if lom.md.Size != size { // corruption or tampering
		return cmn.NewErrLmetaCorrupted(lom.whingeSize(size))
	}
	lom.md.Atime = atimefs
	lom.md.atimefs = uint64(atimefs)

	return nil
}

func (lom *LOM) whingeSize(size int64) error {
	return fmt.Errorf("errsize (%d != %d)", lom.md.Size, size)
}

//
// lock/unlock ------------------------------------------
//

func (lom *LOM) getLocker() *nlc { return &g.locker[lom.CacheIdx()] } // (lif.getLocker())

// returns {apc.LockNone, ...} enum
func (lom *LOM) IsLocked() int {
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

//
// file name too long (0x24) ----------------------------
//

func (lom *LOM) IsFntl() bool {
	return lom.md.lid.haslmfl(lmflFntl)
}

// (compare with fs/content Gen())
func (lom *LOM) ShortenFntl() []string {
	debug.Assert(fs.IsFntl(lom.ObjName), lom.FQN)

	noname := fs.ShortenFntl(lom.FQN)
	nfqn := lom.mi.MakePathFQN(lom.Bucket(), fs.ObjectType, noname)

	debug.Assert(len(nfqn) < 4096, "PATH_MAX /usr/include/limits.h", len(nfqn))
	return []string{nfqn, noname}
}

func (lom *LOM) fixupFntl() {
	if !fs.IsFntl(lom.ObjName) {
		return
	}
	lom.ObjName = fs.ShortenFntl(lom.FQN)                                  // noname
	lom.FQN = lom.mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName) // nfqn
	lom.HrwFQN = &lom.FQN
}

func (lom *LOM) OrigFntl() []string {
	debug.Assert(lom.IsFntl())
	ofqn, ok := lom.GetCustomKey(cmn.OrigFntl)
	if !ok {
		debug.Assert(false)
		return nil
	}
	var parsed fs.ParsedFQN
	if err := parsed.Init(ofqn); err != nil {
		debug.Assert(false)
		return nil
	}
	return []string{ofqn, parsed.ObjName}
}

func (lom *LOM) PushFntl(short []string) (saved []string) {
	saved = []string{lom.FQN, lom.ObjName}
	lom.FQN, lom.ObjName = short[0], short[1]
	lom.HrwFQN = &lom.FQN
	return saved
}

func (lom *LOM) PopFntl(saved []string) {
	lom.FQN, lom.ObjName = saved[0], saved[1]
	lom.HrwFQN = &lom.FQN
}
