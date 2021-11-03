// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/transport"
)

//
// Local Object Metadata (LOM) is a locally stored object metadata comprising:
// - version, atime, checksum, size, etc. object attributes and flags
// - user and internally visible object names
// - associated runtime context including properties and configuration of the
//   bucket that contains the object, etc.
//

const (
	fmtNestedErr      = "nested err: %v"
	lomInitialVersion = "1"
	lomDirtyMask      = uint64(1 << 63)
)

type (
	lmeta struct {
		cmn.ObjAttrs
		uname   string
		atimefs uint64 // high bit is reserved for `dirty`
		bckID   uint64 // see ais/bucketmeta
		copies  fs.MPI // ditto
	}
	LOM struct {
		md          lmeta             // local persistent metadata
		bck         *Bck              // bucket
		mpathInfo   *fs.MountpathInfo // object's mountpath
		mpathDigest uint64            // mountpath's digest
		FQN         string            // fqn
		ObjName     string            // object name in the bucket
		HrwFQN      string            // => main replica (misplaced?)
		info        string
	}
	// LOM In Flight (LIF)
	LIF struct {
		Uname string
		BID   uint64
	}

	ObjectFilter func(*LOM) bool
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
)

func Init(t Target) {
	initBckLocker()
	if t == nil {
		return
	}
	initLomLocker()
	initLomCacheHK(t.MMSA(), t)
	T = t
}

func initLomLocker() {
	lomLocker = make(nameLocker, cos.MultiSyncMapCount)
	lomLocker.init()
	maxLmeta.Store(xattrMaxSize)
}

/////////////
// lomPool //
/////////////

var (
	lomPool sync.Pool
	lom0    LOM
)

func AllocLOM(objName string) (lom *LOM) {
	lom = _allocLOM()
	lom.ObjName = objName
	return
}

func AllocLOMbyFQN(fqn string) (lom *LOM) {
	debug.Assert(strings.Contains(fqn, "/"))
	lom = _allocLOM()
	lom.FQN = fqn
	return
}

func _allocLOM() (lom *LOM) {
	if v := lomPool.Get(); v != nil {
		lom = v.(*LOM)
	} else {
		lom = &LOM{}
	}
	return
}

func FreeLOM(lom *LOM) {
	debug.Assert(lom.ObjName != "" || lom.FQN != "")
	*lom = lom0
	lomPool.Put(lom)
}

/////////
// LIF //
/////////

// LIF => LOF with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	b, objName := cmn.ParseUname(lif.Uname)
	lom = AllocLOM(objName)
	if err = lom.Init(b); err != nil {
		return
	}
	if bprops := lom.Bprops(); bprops == nil {
		err = cmn.NewErrObjDefunct(lom.String(), 0, lif.BID)
	} else if bprops.BID != lif.BID {
		err = cmn.NewErrObjDefunct(lom.String(), bprops.BID, lif.BID)
	}
	return
}

func (lom *LOM) LIF() (lif LIF) {
	debug.Assert(lom.md.uname != "")
	debug.Assert(lom.Bprops() != nil && lom.Bprops().BID != 0)
	return LIF{lom.md.uname, lom.Bprops().BID}
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
	debug.Assert(len(special) > 0 || lom.loaded())
	return lom.md.Size
}

func (lom *LOM) Version(special ...bool) string {
	debug.Assert(len(special) > 0 || lom.loaded())
	return lom.md.Ver
}

func (lom *LOM) Uname() string             { return lom.md.uname }
func (lom *LOM) SetSize(size int64)        { lom.md.Size = size }
func (lom *LOM) SetVersion(ver string)     { lom.md.Ver = ver }
func (lom *LOM) Checksum() *cos.Cksum      { return lom.md.Cksum }
func (lom *LOM) SetCksum(cksum *cos.Cksum) { lom.md.Cksum = cksum }
func (lom *LOM) Atime() time.Time          { return time.Unix(0, lom.md.Atime) }
func (lom *LOM) AtimeUnix() int64          { return lom.md.Atime }
func (lom *LOM) SetAtimeUnix(tu int64)     { lom.md.Atime = tu }

func (lom *LOM) EqCksum(cksum *cos.Cksum) bool { return lom.md.Cksum.Equal(cksum) }

// custom metadata
func (lom *LOM) GetCustomMD() cos.SimpleKVs   { return lom.md.GetCustomMD() }
func (lom *LOM) SetCustomMD(md cos.SimpleKVs) { lom.md.SetCustomMD(md) }

func (lom *LOM) GetCustomKey(key string) (string, bool) { return lom.md.GetCustomKey(key) }
func (lom *LOM) SetCustomKey(key, value string)         { lom.md.SetCustomKey(key, value) }

// lom <= transport.ObjHdr (NOTE: caller must call freeLOM)
func AllocLomFromHdr(hdr *transport.ObjHdr) (lom *LOM, err error) {
	lom = AllocLOM(hdr.ObjName)
	if err = lom.Init(hdr.Bck); err != nil {
		return
	}
	lom.CopyAttrs(&hdr.ObjAttrs, false /*skip checksum*/)
	return
}

func (lom *LOM) ECEnabled() bool { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsHRW() bool     { return lom.HrwFQN == lom.FQN } // subj to resilvering

func (lom *LOM) Bck() *Bck                { return lom.bck }
func (lom *LOM) Bprops() *cmn.BucketProps { return lom.bck.Props }

func (lom *LOM) MirrorConf() *cmn.MirrorConf  { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf    { return lom.bck.CksumConf() }
func (lom *LOM) VersionConf() cmn.VersionConf { return lom.bck.VersionConf() }

// as fs.PartsFQN
func (lom *LOM) ObjectName() string           { return lom.ObjName }
func (lom *LOM) Bucket() cmn.Bck              { return lom.bck.Bucket() } // as fs.PartsFQN
func (lom *LOM) MpathInfo() *fs.MountpathInfo { return lom.mpathInfo }

// see also: transport.ObjHdr.FullName()
func (lom *LOM) FullName() string { return filepath.Join(lom.bck.Name, lom.ObjName) }

func (lom *LOM) WritePolicy() (p cmn.MDWritePolicy) {
	if bprops := lom.Bprops(); bprops == nil {
		p = cmn.WriteImmediate
	} else {
		p = bprops.MDWrite
	}
	return
}

func (lom *LOM) loaded() bool { return lom.md.bckID != 0 }

func (lom *LOM) HrwTarget(smap *Smap) (tsi *Snode, local bool, err error) {
	tsi, err = HrwTarget(lom.Uname(), smap)
	if err != nil {
		return
	}
	local = tsi.ID() == T.Snode().ID()
	return
}

/////////////////////
// copy management //
/////////////////////

func (lom *LOM) _whingeCopy() (yes bool) {
	if !lom.IsCopy() {
		return
	}
	msg := fmt.Sprintf("unexpected: %s([fqn=%s] [hrw=%s] %+v)", lom, lom.FQN, lom.HrwFQN, lom.md.copies)
	debug.AssertMsg(false, msg)
	glog.Error(msg)
	return true
}

func (lom *LOM) HasCopies() bool { return lom.NumCopies() > 1 }

// NumCopies returns a total number of copies: other  mountpaths + self.
func (lom *LOM) NumCopies() int {
	if len(lom.md.copies) == 0 {
		return 1
	}
	debug.AssertFunc(func() (ok bool) {
		_, ok = lom.md.copies[lom.FQN]
		if !ok {
			glog.Errorf("missing self (%s) in copies %v", lom.FQN, lom.md.copies)
		}
		return
	})
	return len(lom.md.copies)
}

// GetCopies returns all copies (NOTE that copies include self)
// NOTE: caller must take a lock
func (lom *LOM) GetCopies() fs.MPI {
	debug.AssertFunc(func() bool {
		rc, exclusive := lom.IsLocked()
		return exclusive || rc > 0
	})
	return lom.md.copies
}

// given an existing (on-disk) object, determines whether it is a _copy_
// (compare with isMirror below)
func (lom *LOM) IsCopy() bool {
	if !lom.HasCopies() || lom.IsHRW() {
		return false
	}
	_, ok := lom.md.copies[lom.FQN]
	debug.Assert(ok)
	return ok
}

// determines whether the two LOM _structures_ represent objects that must be _copies_ of each other
// (compare with IsCopy above)
func (lom *LOM) isMirror(dst *LOM) bool {
	return lom.MirrorConf().Enabled &&
		lom.ObjName == dst.ObjName &&
		lom.Bck().Equal(dst.Bck(), true /* must have same BID*/, true /* same backend */)
}

func (lom *LOM) delCopyMd(copyFQN string) {
	delete(lom.md.copies, copyFQN)
	if len(lom.md.copies) <= 1 {
		lom.md.copies = nil
	}
}

// NOTE: used only in tests
func (lom *LOM) AddCopy(copyFQN string, mpi *fs.MountpathInfo) error {
	if lom.md.copies == nil {
		lom.md.copies = make(fs.MPI, 2)
	}
	lom.md.copies[copyFQN] = mpi
	lom.md.copies[lom.FQN] = lom.mpathInfo
	return lom.syncMetaWithCopies()
}

func (lom *LOM) DelCopies(copiesFQN ...string) (err error) {
	numCopies := lom.NumCopies()
	// 1. Delete all copies from the metadata
	for _, copyFQN := range copiesFQN {
		if _, ok := lom.md.copies[copyFQN]; !ok {
			return fmt.Errorf("lom %s(num: %d): copy %s does not exist", lom, numCopies, copyFQN)
		}
		lom.delCopyMd(copyFQN)
	}

	// 2. Update metadata on remaining copies, if any
	if err := lom.syncMetaWithCopies(); err != nil {
		debug.AssertNoErr(err)
		return err
	}

	// 3. Remove the copies
	for _, copyFQN := range copiesFQN {
		if err1 := cos.RemoveFile(copyFQN); err1 != nil {
			glog.Error(err1) // TODO: LRU should take care of that later.
			continue
		}
	}
	return
}

func (lom *LOM) DelAllCopies() (err error) {
	copiesFQN := make([]string, 0, len(lom.md.copies))
	for copyFQN := range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		copiesFQN = append(copiesFQN, copyFQN)
	}
	return lom.DelCopies(copiesFQN...)
}

// DelExtraCopies deletes objects which are not part of lom.md.copies metadata (leftovers).
func (lom *LOM) DelExtraCopies(fqn ...string) (removed bool, err error) {
	if lom._whingeCopy() {
		return
	}
	availablePaths, _ := fs.Get()
	for _, mi := range availablePaths {
		copyFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
		if _, ok := lom.md.copies[copyFQN]; ok {
			continue
		}
		if err1 := cos.RemoveFile(copyFQN); err1 != nil {
			err = err1
			continue
		}
		if len(fqn) > 0 && fqn[0] == copyFQN {
			removed = true
		}
	}
	return
}

// syncMetaWithCopies tries to make sure that all copies have identical metadata.
// NOTE: uname for LOM must be already locked.
// NOTE: changes _may_ be made - the caller must call lom.Persist() upon return
func (lom *LOM) syncMetaWithCopies() (err error) {
	var copyFQN string
	if !lom.HasCopies() {
		return nil
	}
	// NOTE: caller is responsible for write-locking
	debug.AssertFunc(func() bool {
		_, exclusive := lom.IsLocked()
		return exclusive
	})
	if !lom.WritePolicy().IsImmediate() {
		lom.md.makeDirty()
		return nil
	}
	for {
		if copyFQN, err = lom.persistMdOnCopies(); err == nil {
			break
		}
		lom.delCopyMd(copyFQN)
		if err1 := fs.Access(copyFQN); err1 != nil && !os.IsNotExist(err1) {
			T.FSHC(err, copyFQN) // TODO: notify scrubber
		}
	}
	return
}

// RestoreObjectFromAny tries to restore the object at its default location.
// Returns true if object exists, false otherwise
// TODO: locking vs concurrent restore: consider (read-lock object + write-lock meta) split
func (lom *LOM) RestoreObjectFromAny() (exists bool) {
	lom.Lock(true)
	if err := lom.Load(true /*cache it*/, true /*locked*/); err == nil {
		lom.Unlock(true)
		return true // nothing to do
	}
	availablePaths, _ := fs.Get()
	buf, slab := T.MMSA().Alloc()
	for path, mi := range availablePaths {
		if path == lom.mpathInfo.Path {
			continue
		}
		fqn := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
		if _, err := os.Stat(fqn); err != nil {
			continue
		}
		dst, err := lom._restore(fqn, buf)
		if err == nil {
			lom.md = dst.md
			exists = true
			FreeLOM(dst)
			break
		}
		if dst != nil {
			FreeLOM(dst)
		}
	}
	lom.Unlock(true)
	slab.Free(buf)
	return
}

func (lom *LOM) _restore(fqn string, buf []byte) (dst *LOM, err error) {
	src := lom.Clone(fqn)
	defer FreeLOM(src)
	if err = src.Init(lom.Bucket()); err != nil {
		return
	}
	if err = src.Load(false /*cache it*/, true /*locked*/); err != nil {
		return
	}
	// restore at default location
	dst, err = src.CopyObject(lom.FQN, buf)
	return
}

// NOTE: caller is responsible for locking
func (lom *LOM) CopyObject(dstFQN string, buf []byte) (dst *LOM, err error) {
	var (
		dstCksum  *cos.CksumHash
		srcCksum  = lom.Checksum()
		cksumType = cos.ChecksumNone
	)
	if !srcCksum.IsEmpty() {
		cksumType = srcCksum.Ty()
	}
	dst = lom.Clone(dstFQN)
	if err = dst.Init(cmn.Bck{}); err != nil {
		return
	}
	dst.md.copies = nil
	if dst.isMirror(lom) {
		// caller must take wlock
		debug.AssertFunc(func() bool {
			_, exclusive := lom.IsLocked()
			return exclusive
		})
		if lom.md.copies != nil {
			dst.md.copies = make(fs.MPI, len(lom.md.copies)+1)
			for fqn, mpi := range lom.md.copies {
				dst.md.copies[fqn] = mpi
			}
		}
	}

	if !dst.Bucket().Equal(lom.Bucket()) {
		// The copy will be in a new bucket - completely separate object. Hence, we have to set initial version.
		dst.SetVersion(lomInitialVersion)
	}

	workFQN := fs.CSM.GenContentFQN(dst, fs.WorkfileType, fs.WorkfilePut)
	_, dstCksum, err = cos.CopyFile(lom.FQN, workFQN, buf, cksumType)
	if err != nil {
		return
	}

	if err = cos.Rename(workFQN, dstFQN); err != nil {
		if errRemove := cos.RemoveFile(workFQN); errRemove != nil {
			glog.Errorf(fmtNestedErr, errRemove)
		}
		return
	}

	if cksumType != cos.ChecksumNone {
		if !dstCksum.Equal(lom.Checksum()) {
			return nil, cos.NewBadDataCksumError(&dstCksum.Cksum, lom.Checksum())
		}
		dst.SetCksum(dstCksum.Clone())
	}

	// persist
	if lom.isMirror(dst) {
		if lom.md.copies == nil {
			lom.md.copies = make(fs.MPI, 2)
			dst.md.copies = make(fs.MPI, 2)
		}
		lom.md.copies[dstFQN], dst.md.copies[dstFQN] = dst.mpathInfo, dst.mpathInfo
		lom.md.copies[lom.FQN], dst.md.copies[lom.FQN] = lom.mpathInfo, lom.mpathInfo
		if err = lom.syncMetaWithCopies(); err != nil {
			if _, ok := lom.md.copies[dst.FQN]; !ok {
				if errRemove := os.Remove(dst.FQN); errRemove != nil {
					glog.Errorf("nested err: %v", errRemove)
				}
			}
			// `lom.syncMetaWithCopies()` may have made changes notwithstanding
			if errPersist := lom.Persist(); errPersist != nil {
				glog.Errorf("nested err: %v", errPersist)
			}
			return
		}
		err = lom.Persist()
	} else if err = dst.Persist(); err != nil {
		if errRemove := os.Remove(dst.FQN); errRemove != nil {
			glog.Errorf("nested err: %v", errRemove)
		}
	}
	return
}

//
// lom.String() and helpers
//

func (lom *LOM) String() string {
	if lom.info != "" {
		return lom.info
	}
	return lom._string(lom.bck.Name)
}

func (lom *LOM) _string(b string) string {
	var (
		a string
		s = "o[" + b + "/" + lom.ObjName
	)
	if glog.FastV(4, glog.SmoduleCluster) {
		s += fmt.Sprintf(" %s (%s)", lom.mpathInfo.Fs, lom.FQN)
		if lom.md.Size != 0 {
			s += " size=" + cos.B2S(lom.md.Size, 1)
		}
		if lom.md.Ver != "" {
			s += " ver=" + lom.md.Ver
		}
		if lom.md.Cksum != nil {
			s += " " + lom.md.Cksum.String()
		}
	}
	if lom.loaded() {
		if lom.IsCopy() {
			a += "(copy)"
		} else if !lom.IsHRW() {
			a += "(misplaced)"
		}
		if n := lom.NumCopies(); n > 1 {
			a += fmt.Sprintf("(%dc)", n)
		}
	} else {
		a = "(-)"
		if !lom.IsHRW() {
			a += "(not-hrw)"
		}
	}
	lom.info = s + a + "]"
	return lom.info
}

// increment ais LOM's version
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

func (lom *LOM) LBGet() (fqn string) {
	if !lom.HasCopies() {
		return lom.FQN
	}
	return lom.bestCopy()
}

// NOTE: reconsider counting GETs (and the associated overhead)
//       vs ios.refreshIostatCache() (and the associated delay)
func (lom *LOM) bestCopy() (fqn string) {
	var (
		mpathUtils = fs.GetAllMpathUtils()
		minUtil    = mpathUtils.Util(lom.mpathInfo.Path)
		copies     = lom.GetCopies()
	)
	fqn = lom.FQN
	for copyFQN, copyMPI := range copies {
		if copyFQN != lom.FQN {
			if util := mpathUtils.Util(copyMPI.Path); util < minUtil {
				fqn, minUtil = copyFQN, util
			}
		}
	}
	return
}

// returns the least utilized mountpath that does _not_ have a copy yet
// (see also bestCopy above)
func (lom *LOM) BestMpath() (mi *fs.MountpathInfo) {
	var (
		availablePaths, _ = fs.Get()
		mpathUtils        = fs.GetAllMpathUtils()
		minUtil           = int64(101)
	)
	for mpath, mpathInfo := range availablePaths {
		if !lom.haveMpath(mpath) {
			if util := mpathUtils.Util(mpath); util < minUtil {
				minUtil, mi = util, mpathInfo
			}
		}
	}
	return
}

func (lom *LOM) haveMpath(mpath string) bool {
	if len(lom.md.copies) == 0 {
		return lom.mpathInfo.Path == mpath
	}
	for _, mi := range lom.md.copies {
		if mi.Path == mpath {
			return true
		}
	}
	return false
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
	if lom.CksumConf().Type == cos.ChecksumNone {
		return nil
	}
	md, err = lom.lmfs(false)
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
		cksumType = lom.CksumConf().Type

		cksums = struct {
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
	if _, err = lom.lmfs(true); err == nil && lom.md.Cksum != nil {
		if cksumType == lom.md.Cksum.Ty() {
			if cksums.comp.Equal(lom.md.Cksum) {
				return
			}
		} else { // type changed
			cksums.stor = lom.md.Cksum
			cksumType = lom.CksumConf().Type
			goto recomp
		}
	}
ex:
	err = cos.NewBadDataCksumError(&cksums.comp.Cksum, cksums.stor, lom.String())
	lom.Uncache(true /*delDirty*/)
	return
}

func (lom *LOM) ComputeCksumIfMissing() (cksum *cos.Cksum, err error) {
	var cksumHash *cos.CksumHash
	if lom.md.Cksum != nil {
		cksum = lom.md.Cksum
		return
	}
	cksumHash, err = lom.ComputeCksum()
	if cksumHash != nil && err == nil {
		cksum = cksumHash.Clone()
		lom.SetCksum(cksum)
	}
	return
}

func (lom *LOM) ComputeCksum(cksumTypes ...string) (cksum *cos.CksumHash, err error) {
	var (
		file      *os.File
		cksumType string
	)
	if len(cksumTypes) > 0 {
		cksumType = cksumTypes[0]
	} else {
		cksumType = lom.CksumConf().Type
	}
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

// NOTE: Clone shallow-copies LOM to be further initialized (lom.Init) for a given replica
//       (mountpath/FQN)
func (lom *LOM) Clone(fqn string) *LOM {
	dst := AllocLOMbyFQN(fqn)
	*dst = *lom
	dst.md = lom.md
	dst.FQN = fqn
	return dst
}

// Local Object Metadata (LOM) - is cached. Respectively, lifecycle of any given LOM
// instance includes the following steps:
// 1) construct LOM instance and initialize its runtime state: lom = LOM{...}.Init()
// 2) load persistent state (aka lmeta) from one of the LOM caches or the underlying
//    filesystem: lom.Load(); Load(false) also entails *not adding* LOM to caches
//    (useful when deleting or moving objects
// 3) usage: lom.Atime(), lom.Cksum(), and other accessors
//    It is illegal to check LOM's existence and, generally, do almost anything
//    with it prior to loading - see previous
// 4) update persistent state in memory: lom.Set*() methods
//    (requires subsequent re-caching via lom.ReCache())
// 5) update persistent state on disk: lom.Persist()
// 6) remove a given LOM instance from cache: lom.Uncache()
// 7) evict an entire bucket-load of LOM cache: cluster.EvictCache(bucket)
// 8) periodic (lazy) eviction followed by access-time synchronization: see LomCacheRunner
// =======================================================================================

func hkIdx(digest uint64) int {
	return int(digest & (cos.MultiSyncMapCount - 1))
}

func (lom *LOM) Hkey() (string, int) {
	return lom.md.uname, hkIdx(lom.mpathDigest)
}

func (lom *LOM) Init(bck cmn.Bck) (err error) {
	if lom.FQN != "" {
		var parsedFQN fs.ParsedFQN
		parsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN)
		if err != nil {
			return
		}
		debug.Assertf(parsedFQN.ContentType == fs.ObjectType,
			"use CT for non-objects[%s]: %s", parsedFQN.ContentType, lom.FQN)
		lom.mpathInfo = parsedFQN.MpathInfo
		lom.mpathDigest = parsedFQN.Digest
		if bck.Name == "" {
			bck.Name = parsedFQN.Bck.Name
		} else if bck.Name != parsedFQN.Bck.Name {
			return fmt.Errorf("lom-init %s: bucket mismatch (%s != %s)", lom.FQN, bck, parsedFQN.Bck)
		}
		lom.ObjName = parsedFQN.ObjName
		prov := parsedFQN.Bck.Provider
		if bck.Provider == "" {
			bck.Provider = prov
		} else if bck.Provider != prov {
			return fmt.Errorf("lom-init %s: provider mismatch (%s != %s)", lom.FQN, bck.Provider, prov)
		}
		if bck.Ns.IsGlobal() {
			bck.Ns = parsedFQN.Bck.Ns
		} else if bck.Ns != parsedFQN.Bck.Ns {
			return fmt.Errorf("lom-init %s: namespace mismatch (%s != %s)",
				lom.FQN, bck.Ns, parsedFQN.Bck.Ns)
		}
	}
	bowner := T.Bowner()
	lom.bck = NewBckEmbed(bck)
	if err = lom.bck.Init(bowner); err != nil {
		return
	}
	lom.md.uname = lom.bck.MakeUname(lom.ObjName)
	if lom.FQN == "" {
		lom.mpathInfo, lom.mpathDigest, err = HrwMpath(lom.md.uname)
		if err != nil {
			return
		}
		lom.FQN = lom.mpathInfo.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
		lom.HrwFQN = lom.FQN
	}
	return
}

// * locked: is locked by the immediate caller (or otherwise is known to be locked);
//   if false, try Rlock temporarily *if and only when* reading from FS
func (lom *LOM) Load(cacheit, locked bool) (err error) {
	var (
		hkey, idx = lom.Hkey()
		lcache    = lom.mpathInfo.LomCache(idx)
		bmd       = T.Bowner().Get()
	)
	if md, ok := lcache.Load(hkey); ok { // fast path
		lmeta := md.(*lmeta)
		lom.md = *lmeta
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
	if cacheit {
		md := &lmeta{}
		*md = lom.md
		lcache.Store(hkey, md)
	}
	return
}

func (lom *LOM) _checkBucket(bmd *BMD) (err error) {
	debug.Assert(lom.loaded())
	err = bmd.Check(lom.bck, lom.md.bckID)
	if err == errBucketIDMismatch {
		err = cmn.NewErrObjDefunct(lom.String(), lom.md.bckID, lom.bck.Props.BID)
	}
	return
}

func (lom *LOM) ReCache(store bool) {
	var (
		hkey, idx = lom.Hkey()
		lcache    = lom.mpathInfo.LomCache(idx)
		md        = &lmeta{}
	)
	if !store {
		_, store = lcache.Load(hkey) // refresh an existing one
	}
	if store {
		*md = lom.md
		md.bckID = lom.Bprops().BID
		lom.md.bckID = md.bckID
		debug.Assert(md.bckID != 0)
		lcache.Store(hkey, md)
	}
}

func (lom *LOM) Uncache(delDirty bool) {
	debug.Assert(!lom.IsCopy()) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		lcache    = lom.mpathInfo.LomCache(idx)
	)
	if delDirty {
		lcache.Delete(hkey)
		return
	}
	if md, ok := lcache.Load(hkey); ok {
		lmeta := md.(*lmeta)
		if lmeta.isDirty() {
			return
		}
	}
	lcache.Delete(hkey)
}

func (lom *LOM) FromFS() (err error) {
	var (
		finfo os.FileInfo
		retry = true
	)
beg:
	finfo, err = os.Stat(lom.FQN)
	if err != nil {
		if !os.IsNotExist(err) {
			err = fmt.Errorf("%s: errstat %w", lom, err)
			T.FSHC(err, lom.FQN)
		}
		return
	}
	if _, err = lom.lmfs(true); err != nil {
		if err == syscall.ENODATA || err == syscall.ENOENT {
			if retry {
				runtime.Gosched()
				retry = false
				goto beg
			}
		}
		err = cmn.NewErrObjMeta(lom.ObjName, err)
		T.FSHC(err, lom.FQN)
		return
	}
	// fstat & atime
	if lom.md.Size != finfo.Size() { // corruption or tampering
		return fmt.Errorf("%s: errsize (%d != %d)", lom, lom.md.Size, finfo.Size())
	}
	atime := ios.GetATime(finfo)
	lom.md.Atime = atime.UnixNano()
	debug.Assert(lom.md.Atime > 0)
	lom.md.atimefs = uint64(lom.md.Atime)
	return
}

func (lom *LOM) Remove() (err error) {
	// caller must take wlock
	debug.AssertFunc(func() bool {
		rc, exclusive := lom.IsLocked()
		return exclusive || rc > 0
	})
	lom.Uncache(true /*delDirty*/)
	err = cos.RemoveFile(lom.FQN)
	for copyFQN := range lom.md.copies {
		if err := cos.RemoveFile(copyFQN); err != nil {
			glog.Error(err)
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
			cache.Range(func(hkey, _ interface{}) bool {
				uname := hkey.(string)
				bck, _ := cmn.ParseUname(uname)
				if bck.Equal(b.Bck) {
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
		i                 int
		availablePaths, _ = fs.Get()
		cachesCnt         = len(availablePaths) * cos.MultiSyncMapCount
		caches            = make([]*sync.Map, cachesCnt)
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

func getLomLocker(idx int) *nlc { return &lomLocker[idx] }

func (lom *LOM) IsLocked() (int /*rc*/, bool /*exclusive*/) {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	return nlc.IsLocked(lom.Uname())
}

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
	nlc.Lock(lom.Uname(), exclusive)
}

func (lom *LOM) UpgradeLock() (finished bool) {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	return nlc.UpgradeLock(lom.Uname())
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

// compare with cos.CreateFile
func (lom *LOM) CreateFile(fqn string) (fh *os.File, err error) {
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err == nil || !os.IsNotExist(err) {
		return
	}
	// slow path
	bdir := lom.mpathInfo.MakePathBck(lom.Bucket())
	if _, err = os.Stat(bdir); err != nil {
		return nil, fmt.Errorf("%s: bucket directory %w", lom, err)
	}
	fdir := filepath.Dir(fqn)
	if err = cos.CreateDir(fdir); err != nil {
		return
	}
	fh, err = os.OpenFile(fqn, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	return
}
