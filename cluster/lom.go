// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
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
)

type (
	lmeta struct {
		uname    string
		version  string
		size     int64
		atime    int64
		atimefs  int64
		bckID    uint64
		cksum    *cmn.Cksum // ReCache(ref)
		copies   fs.MPI     // ditto
		customMD cmn.SimpleKVs
		dirty    bool
	}
	LOM struct {
		md          lmeta             // local meta
		bck         *Bck              // bucket
		mpathInfo   *fs.MountpathInfo // object's mountpath
		mpathDigest uint64            // mountpath's digest
		FQN         string            // fqn
		ObjName     string            // object name in the bucket
		HrwFQN      string            // (misplaced?)
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
	_ cmn.ObjHeaderMetaProvider = (*LOM)(nil)
	_ fs.PartsFQN               = (*LOM)(nil)
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
	lomLocker = make(nameLocker, cmn.MultiSyncMapCount)
	lomLocker.init()
	maxLmeta.Store(xattrMaxSize)
}

//
// LOM public methods
//

func (lom *LOM) Uname() string                { return lom.md.uname }
func (lom *LOM) Size() int64                  { return lom.md.size }
func (lom *LOM) SetSize(size int64)           { lom.md.size = size }
func (lom *LOM) Version() string              { return lom.md.version }
func (lom *LOM) SetVersion(ver string)        { lom.md.version = ver }
func (lom *LOM) Cksum() *cmn.Cksum            { return lom.md.cksum }
func (lom *LOM) SetCksum(cksum *cmn.Cksum)    { lom.md.cksum = cksum }
func (lom *LOM) Atime() time.Time             { return time.Unix(0, lom.md.atime) }
func (lom *LOM) AtimeUnix() int64             { return lom.md.atime }
func (lom *LOM) SetAtimeUnix(tu int64)        { lom.md.atime = tu }
func (lom *LOM) SetCustomMD(md cmn.SimpleKVs) { lom.md.customMD = md }
func (lom *LOM) CustomMD() cmn.SimpleKVs      { return lom.md.customMD }
func (lom *LOM) GetCustomMD(key string) (string, bool) {
	value, exists := lom.md.customMD[key]
	return value, exists
}
func (lom *LOM) ECEnabled() bool { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsHRW() bool     { return lom.HrwFQN == lom.FQN } // subj to resilvering

func (lom *LOM) ObjectName() string           { return lom.ObjName }
func (lom *LOM) Bck() *Bck                    { return lom.bck }
func (lom *LOM) BckName() string              { return lom.bck.Name }
func (lom *LOM) Bprops() *cmn.BucketProps     { return lom.bck.Props }
func (lom *LOM) Bucket() cmn.Bck              { return lom.bck.Bck }
func (lom *LOM) MpathInfo() *fs.MountpathInfo { return lom.mpathInfo }

func (lom *LOM) MirrorConf() *cmn.MirrorConf  { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf    { return lom.bck.CksumConf() }
func (lom *LOM) VersionConf() cmn.VersionConf { return lom.bck.VersionConf() }

func (lom *LOM) WritePolicy() (p cmn.MDWritePolicy) {
	if bprops := lom.Bprops(); bprops == nil {
		p = cmn.WriteImmediate
	} else {
		p = bprops.MDWrite
	}
	return
}

func (lom *LOM) loaded() bool { return lom.md.bckID != 0 }

// see also: transport.FromHdrProvider()
func (lom *LOM) ToHTTPHdr(hdr http.Header) http.Header {
	return cmn.ToHTTPHdr(lom, hdr)
}

func (lom *LOM) FromHTTPHdr(hdr http.Header) {
	// NOTE: not setting received checksum into the LOM
	// to preserve its own local checksum

	if versionEntry := hdr.Get(cmn.HeaderObjVersion); versionEntry != "" {
		lom.SetVersion(versionEntry)
	}
	if atimeEntry := hdr.Get(cmn.HeaderObjAtime); atimeEntry != "" {
		atime, _ := cmn.S2UnixNano(atimeEntry)
		lom.SetAtimeUnix(atime)
	}
	if customMD := hdr[http.CanonicalHeaderKey(cmn.HeaderObjCustomMD)]; len(customMD) > 0 {
		md := make(cmn.SimpleKVs, len(customMD)*2)
		for _, v := range customMD {
			entry := strings.SplitN(v, "=", 2)
			cmn.Assert(len(entry) == 2)
			md[entry[0]] = entry[1]
		}
		lom.SetCustomMD(md)
	}
}

////////////////////////////
// LOCAL COPY MANAGEMENT //
///////////////////////////

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

// NumCopies returns number of copies: different mountpaths + itself.
func (lom *LOM) NumCopies() int {
	if len(lom.md.copies) == 0 {
		return 1
	}
	debug.AssertFunc(func() (ok bool) {
		_, ok = lom.md.copies[lom.FQN]
		if !ok {
			glog.Errorf("%s: missing self (%s) in copies %v", lom, lom.FQN, lom.md.copies)
		}
		return
	})
	return len(lom.md.copies)
}

// GetCopies returns all copies (and NOTE that copies include self)
func (lom *LOM) GetCopies() fs.MPI { return lom.md.copies }

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
			return fmt.Errorf("lom %s(num: %d): copy %s %s", lom, numCopies, copyFQN, cmn.DoesNotExist)
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
		if err1 := cmn.RemoveFile(copyFQN); err1 != nil {
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
	for _, mpathInfo := range availablePaths {
		copyFQN := fs.CSM.FQN(mpathInfo, lom.bck.Bck, fs.ObjectType, lom.ObjName)
		if _, ok := lom.md.copies[copyFQN]; ok {
			continue
		}
		if err1 := cmn.RemoveFile(copyFQN); err1 != nil {
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
	if lom.WritePolicy() != cmn.WriteImmediate {
		lom.md.dirty = true
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
	if err := lom.Load(false); err == nil {
		lom.Unlock(true)
		return true // nothing to do
	}

	availablePaths, _ := fs.Get()
	buf, slab := T.MMSA().Alloc()
	for path, mpathInfo := range availablePaths {
		if path == lom.mpathInfo.Path {
			continue
		}
		fqn := fs.CSM.FQN(mpathInfo, lom.bck.Bck, fs.ObjectType, lom.ObjName)
		if _, err := os.Stat(fqn); err != nil {
			continue
		}
		src := lom.Clone(fqn)
		if err := src.Init(lom.bck.Bck); err != nil {
			continue
		}
		if err := src.Load(false); err != nil {
			continue
		}
		// restore at default location
		dst, err := src.CopyObject(lom.FQN, buf)
		if err == nil {
			lom.md = dst.md
			exists = true
			break
		}
	}
	lom.Unlock(true)
	slab.Free(buf)
	return
}

// NOTE: caller is responsible for locking
func (lom *LOM) CopyObject(dstFQN string, buf []byte) (dst *LOM, err error) {
	var (
		dstCksum  *cmn.CksumHash
		srcCksum  = lom.Cksum()
		cksumType = cmn.ChecksumNone
	)
	if srcCksum != nil {
		cksumType = srcCksum.Type()
	}

	dst = lom.Clone(dstFQN)
	if err = dst.Init(cmn.Bck{}); err != nil {
		return
	}
	dst.md.copies = nil
	if dst.isMirror(lom) {
		// NOTE: caller is responsible for write-locking
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

	workFQN := fs.CSM.GenContentFQN(dst, fs.WorkfileType, fs.WorkfilePut)
	_, dstCksum, err = cmn.CopyFile(lom.FQN, workFQN, buf, cksumType)
	if err != nil {
		return
	}

	if err = cmn.Rename(workFQN, dstFQN); err != nil {
		if errRemove := cmn.RemoveFile(workFQN); errRemove != nil {
			glog.Errorf(fmtNestedErr, errRemove)
		}
		return
	}

	if cksumType != cmn.ChecksumNone {
		if !dstCksum.Equal(lom.Cksum()) {
			return nil, cmn.NewBadDataCksumError(&dstCksum.Cksum, lom.Cksum())
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

func (lom *LOM) String() string { return lom._string(lom.bck.Name) }

func (lom *LOM) _string(b string) string {
	var (
		a string
		s = fmt.Sprintf("o[%s/%s fs=%s", b, lom.ObjName, lom.mpathInfo.FileSystem)
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
	return s + a + "]"
}

// increment ais LOM's version
func (lom *LOM) IncVersion() error {
	cmn.Assert(lom.Bck().IsAIS())
	if lom.Version() == "" {
		lom.SetVersion(lomInitialVersion)
		return nil
	}
	ver, err := strconv.Atoi(lom.Version())
	if err != nil {
		return err
	}
	lom.SetVersion(strconv.Itoa(ver + 1))
	return nil
}

// best-effort GET load balancing (see also mirror.findLeastUtilized())
func (lom *LOM) LoadBalanceGET() (fqn string) {
	if !lom.HasCopies() {
		return lom.FQN
	}
	return fs.LoadBalanceGET(lom.FQN, lom.mpathInfo.Path, lom.GetCopies())
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
	if lom.CksumConf().Type == cmn.ChecksumNone {
		return nil
	}
	md, err = lom.lmfs(false)
	if err != nil {
		return err
	}
	if md == nil {
		return fmt.Errorf("%s: no meta", lom)
	}
	if lom.md.cksum == nil {
		lom.SetCksum(md.cksum)
		return nil
	}
	// different versions may have different checksums
	if md.version == lom.md.version && !lom.md.cksum.Equal(md.cksum) {
		err = cmn.NewBadDataCksumError(lom.md.cksum, md.cksum, lom.String())
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
			stor *cmn.Cksum     // stored with LOM
			comp *cmn.CksumHash // computed
		}{stor: lom.md.cksum}

		reloaded bool
	)
recomp:
	if cksumType == cmn.ChecksumNone { // as far as do-no-checksum-checking bucket rules
		return
	}
	if !lom.md.cksum.IsEmpty() {
		cksumType = lom.md.cksum.Type() // takes precedence on the other hand
	}
	if cksums.comp, err = lom.ComputeCksum(cksumType); err != nil {
		return
	}
	if lom.md.cksum.IsEmpty() { // store computed
		lom.md.cksum = cksums.comp.Clone()
		if err = lom.Persist(); err != nil {
			lom.md.cksum = cksums.stor
		}
		return
	}
	if cksums.comp.Equal(lom.md.cksum) {
		return
	}
	if reloaded {
		goto ex
	}
	// retry: load from disk and check again
	reloaded = true
	if _, err = lom.lmfs(true); err == nil && lom.md.cksum != nil {
		if cksumType == lom.md.cksum.Type() {
			if cksums.comp.Equal(lom.md.cksum) {
				return
			}
		} else { // type changed
			cksums.stor = lom.md.cksum
			cksumType = lom.CksumConf().Type
			goto recomp
		}
	}
ex:
	err = cmn.NewBadDataCksumError(&cksums.comp.Cksum, cksums.stor, lom.String())
	lom.Uncache(true /*delDirty*/)
	return
}

func (lom *LOM) ComputeCksumIfMissing() (cksum *cmn.Cksum, err error) {
	var cksumHash *cmn.CksumHash
	if lom.md.cksum != nil {
		cksum = lom.md.cksum
		return
	}
	cksumHash, err = lom.ComputeCksum()
	if cksumHash != nil && err == nil {
		cksum = cksumHash.Clone()
		lom.SetCksum(cksum)
	}
	return
}

func (lom *LOM) ComputeCksum(cksumTypes ...string) (cksum *cmn.CksumHash, err error) {
	var (
		file      *os.File
		cksumType string
	)
	if len(cksumTypes) > 0 {
		cksumType = cksumTypes[0]
	} else {
		cksumType = lom.CksumConf().Type
	}
	if cksumType == cmn.ChecksumNone {
		return
	}
	if file, err = os.Open(lom.FQN); err != nil {
		return
	}
	// No need to allocate `buf` as `ioutil.Discard` has efficient `io.ReaderFrom` implementation.
	_, cksum, err = cmn.CopyAndChecksum(ioutil.Discard, file, nil, cksumType)
	cmn.Close(file)
	if err != nil {
		return nil, err
	}
	return
}

// NOTE: Clone performs a shallow copy of the LOM.
func (lom *LOM) Clone(fqn string) *LOM {
	dst := &LOM{}
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
func (lom *LOM) Hkey() (string, int) {
	debug.Assert(lom.mpathDigest != 0)
	return lom.md.uname, int(lom.mpathDigest & (cmn.MultiSyncMapCount - 1))
}

func (lom *LOM) Init(bck cmn.Bck) (err error) {
	if lom.FQN != "" {
		var parsedFQN fs.ParsedFQN
		parsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN)
		if err != nil {
			return
		}
		if parsedFQN.ContentType != fs.ObjectType {
			debug.Assertf(parsedFQN.ContentType == fs.ObjectType, "use CT for non-objects[%s]: %s", parsedFQN.ContentType, lom.FQN)
		}
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
			return fmt.Errorf("lom-init %s: namespace mismatch (%s != %s)", lom.FQN, bck.Ns, parsedFQN.Bck.Ns)
		}
	}
	bowner := T.Bowner()
	lom.bck = NewBckEmbed(bck)
	if err = lom.bck.Init(bowner, T.Snode()); err != nil {
		return
	}
	lom.md.uname = lom.bck.MakeUname(lom.ObjName)
	if lom.FQN == "" {
		lom.mpathInfo, lom.mpathDigest, err = HrwMpath(lom.md.uname)
		if err != nil {
			return
		}
		lom.FQN = fs.CSM.FQN(lom.mpathInfo, lom.bck.Bck, fs.ObjectType, lom.ObjName)
		lom.HrwFQN = lom.FQN
	}
	return
}

func (lom *LOM) IsLoaded() (ok bool) {
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.mpathInfo.LomCache(idx)
	)
	_, ok = cache.Load(hkey)
	return
}

func (lom *LOM) Load(adds ...bool) (err error) {
	// fast path
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.mpathInfo.LomCache(idx)
		add       = true // default: cache it
	)
	if len(adds) > 0 {
		add = adds[0]
	}
	if md, ok := cache.Load(hkey); ok { // fast path
		lmeta := md.(*lmeta)
		lom.md = *lmeta
		err = lom.checkBucket()
		return
	}
	err = lom.FromFS() // slow path
	if err == nil {
		if lom.Bprops().BID == 0 {
			return
		}
		lom.md.bckID = lom.Bprops().BID
		err = lom.checkBucket()
		if err == nil && add {
			md := &lmeta{}
			*md = lom.md
			cache.Store(hkey, md)
		}
	}
	return
}

func (lom *LOM) checkBucket() error {
	debug.Assert(lom.loaded()) // cannot check bucket without first calling lom.Load()
	var (
		bmd             = T.Bowner().Get()
		bprops, present = bmd.Get(lom.bck)
	)
	if !present { // bucket does not exist
		node := T.Snode().String()
		if lom.bck.IsRemote() {
			return cmn.NewErrorRemoteBucketDoesNotExist(lom.Bck().Bck, node)
		}
		return cmn.NewErrorBucketDoesNotExist(lom.Bck().Bck, node)
	}
	if lom.md.bckID == bprops.BID {
		return nil // ok
	}
	lom.Uncache(true /*delDirty*/)
	return cmn.NewObjDefunctError(lom.String(), lom.md.bckID, lom.bck.Props.BID)
}

func (lom *LOM) ReCache(store bool) {
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.mpathInfo.LomCache(idx)
		md        = &lmeta{}
	)
	if !store {
		_, store = cache.Load(hkey) // refresh an existing one
	}
	if store {
		*md = lom.md
		md.bckID = lom.Bprops().BID
		debug.Assert(md.bckID != 0)
		cache.Store(hkey, md)
	}
}

func (lom *LOM) Uncache(delDirty bool) {
	debug.Assert(!lom.IsCopy()) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.mpathInfo.LomCache(idx)
	)
	if delDirty {
		cache.Delete(hkey)
		return
	}
	if md, ok := cache.Load(hkey); ok {
		lmeta := md.(*lmeta)
		if lmeta.dirty {
			return
		}
	}
	cache.Delete(hkey)
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
		err = cmn.NewObjMetaErr(lom.ObjName, err)
		T.FSHC(err, lom.FQN)
		return
	}
	// fstat & atime
	if lom.md.size != finfo.Size() { // corruption or tampering
		return fmt.Errorf("%s: errsize (%d != %d)", lom, lom.md.size, finfo.Size())
	}
	atime := ios.GetATime(finfo)
	lom.md.atime = atime.UnixNano()
	lom.md.atimefs = lom.md.atime
	return
}

func (lom *LOM) Remove() (err error) {
	lom.Uncache(true /*delDirty*/)
	err = cmn.RemoveFile(lom.FQN)
	for copyFQN := range lom.md.copies {
		if err := cmn.RemoveFile(copyFQN); err != nil {
			glog.Error(err)
		}
	}
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
	for _, cache := range caches {
		wg.Add(1)
		go func(cache *sync.Map) {
			cache.Range(func(hkey, _ interface{}) bool {
				uname := hkey.(string)
				bck, _ := parseUname(uname)
				if bck.Bck.Equal(b.Bck) {
					cache.Delete(hkey)
				}
				return true
			})
			wg.Done()
		}(cache)
	}
	wg.Wait()
}

//
// static helpers
//
func lomFromLmeta(md *lmeta, bmd *BMD) (lom *LOM, bucketExists bool) {
	var (
		bck, objName = parseUname(md.uname)
		err          error
	)
	lom = &LOM{ObjName: objName, bck: &bck}
	bucketExists = bmd.Exists(&bck, md.bckID)
	if bucketExists {
		lom.FQN, _, err = HrwFQN(lom.Bck(), fs.ObjectType, lom.ObjName)
		if err != nil {
			glog.Errorf("%s: hrw err: %v", lom, err)
			bucketExists = false
		}
	}
	return
}

func lomCaches() []*sync.Map {
	var (
		availablePaths, _ = fs.Get()
		cachesCnt         = len(availablePaths) * cmn.MultiSyncMapCount
		caches            = make([]*sync.Map, cachesCnt)
		i                 = 0
	)
	for _, mpathInfo := range availablePaths {
		for idx := 0; idx < cmn.MultiSyncMapCount; idx++ {
			caches[i] = mpathInfo.LomCache(idx)
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

func (lom *LOM) UpgradeLock() {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	nlc.UpgradeLock(lom.Uname())
}

func (lom *LOM) DowngradeLock() {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	nlc.DowngradeLock(lom.Uname())
}

func (lom *LOM) TryUpgradeLock() bool {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	return nlc.TryUpgradeLock(lom.Uname())
}

func (lom *LOM) Unlock(exclusive bool) {
	var (
		_, idx = lom.Hkey()
		nlc    = getLomLocker(idx)
	)
	nlc.Unlock(lom.Uname(), exclusive)
}

//
// create file
//

func (lom *LOM) CreateFile(fqn string) (*os.File, error) {
	var (
		mi  = lom.mpathInfo
		dir = mi.MakePathBck(lom.Bck().Bck)
	)
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("failed to create %s: bucket directory %s %w", fqn, dir, err)
	}
	fh, err := cmn.CreateFile(fqn)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create %s: %w", lom, fqn, err)
	}
	return fh, nil
}
