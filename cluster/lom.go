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
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
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

const fmtNestedErr = "nested err: %v"
const lomInitialVersion = "1"

type (
	// NOTE: sizeof(lmeta) = 88 as of 5/26
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
	}
	LOM struct {
		md      lmeta  // local meta
		bck     *Bck   // bucket
		FQN     string // fqn
		ObjName string // object name in the bucket
		HrwFQN  string // (misplaced?)
		// runtime
		T         Target
		config    *cmn.Config
		ParsedFQN fs.ParsedFQN // redundant in-part; tradeoff to speed-up workfile name gen, etc.
		loaded    bool
	}

	ObjectFilter func(*LOM) bool
)

var (
	lomLocker nameLocker
	maxLmeta  atomic.Int64
)

func InitTarget() {
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
func (lom *LOM) ECEnabled() bool            { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsHRW() bool                { return lom.HrwFQN == lom.FQN } // subj to resilvering
func (lom *LOM) Bck() *Bck                  { return lom.bck }
func (lom *LOM) BckName() string            { return lom.bck.Name }
func (lom *LOM) Bprops() *cmn.BucketProps   { return lom.bck.Props }
func (lom *LOM) GetFQN() string             { return lom.FQN }
func (lom *LOM) GetParsedFQN() fs.ParsedFQN { return lom.ParsedFQN }

func (lom *LOM) Config() *cmn.Config {
	if lom.config == nil {
		lom.config = cmn.GCO.Get()
	}
	return lom.config
}
func (lom *LOM) MirrorConf() *cmn.MirrorConf { return &lom.Bprops().Mirror }
func (lom *LOM) CksumConf() *cmn.CksumConf   { return lom.bck.CksumConf() }
func (lom *LOM) VerConf() *cmn.VersionConf   { return &lom.Bprops().Versioning }

func (lom *LOM) CopyMetadata(from *LOM) {
	lom.md.copies = nil
	if lom.MirrorConf().Enabled && lom.Bck().Equal(from.Bck(), true /* must have same BID*/) {
		lom.setCopiesMd(from.FQN, from.ParsedFQN.MpathInfo)
		for fqn, mpathInfo := range from.GetCopies() {
			lom.addCopyMd(fqn, mpathInfo)
		}
		// TODO: in future we should have invariant: cmn.Assert(lom.NumCopies() == from.NumCopies())
	}

	lom.md.cksum = from.md.cksum
	lom.md.size = from.md.size
	lom.md.version = from.md.version
	lom.md.atime = from.md.atime
}

func (lom *LOM) CloneCopiesMd() int {
	var (
		num    = len(lom.md.copies)
		copies = lom.md.copies
	)
	lom.md.copies = make(fs.MPI, num)
	for fqn, mpi := range copies {
		lom.md.copies[fqn] = mpi
	}
	return num
}

func (lom *LOM) PopulateHdr(hdr http.Header) http.Header {
	if hdr == nil {
		hdr = make(http.Header, 4)
	}
	if lom.Cksum() != nil {
		if ty, val := lom.Cksum().Get(); ty != cmn.ChecksumNone {
			hdr.Set(cmn.HeaderObjCksumType, ty)
			hdr.Set(cmn.HeaderObjCksumVal, val)
		}
	}
	if lom.Version() != "" {
		hdr.Set(cmn.HeaderObjVersion, lom.Version())
	}
	if lom.AtimeUnix() != 0 {
		hdr.Set(cmn.HeaderObjAtime, cmn.UnixNano2S(lom.AtimeUnix()))
	}
	for k, v := range lom.CustomMD() {
		hdr.Add(cmn.HeaderObjCustomMD, strings.Join([]string{k, v}, "="))
	}
	return hdr
}
func (lom *LOM) ParseHdr(hdr http.Header) {
	// NOTE: We never set the `Cksum` from the header
	//  (although we send it) - it should be computed.

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
//
// TODO -- FIXME: lom.md.copies are not protected and are subject to races
//                the caller to use lom.Lock/Unlock (as in mirror)

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
	if debug.Enabled {
		_, ok := lom.md.copies[lom.FQN]
		debug.Assertf(ok, "md.copies does not contain itself %s, copies: %v", lom.FQN, lom.md.copies)
	}
	return len(lom.md.copies)
}

// GetCopies returns all copies.
//
// NOTE: This method can also return the mpath/FQN of current lom.
// This means that the caller is responsible for filtering it out if necessary!
func (lom *LOM) GetCopies() fs.MPI {
	return lom.md.copies
}

func (lom *LOM) IsCopy() bool {
	if lom.IsHRW() {
		return false
	}
	_, ok := lom.md.copies[lom.FQN]
	return ok
}

func (lom *LOM) setCopiesMd(copyFQN string, mpi *fs.MountpathInfo) {
	lom.md.copies = make(fs.MPI, 2)
	lom.md.copies[copyFQN] = mpi
	lom.md.copies[lom.FQN] = lom.ParsedFQN.MpathInfo
}
func (lom *LOM) addCopyMd(copyFQN string, mpi *fs.MountpathInfo) {
	if lom.md.copies == nil {
		lom.md.copies = make(fs.MPI, 2)
	}
	lom.md.copies[copyFQN] = mpi
	lom.md.copies[lom.FQN] = lom.ParsedFQN.MpathInfo
}
func (lom *LOM) delCopyMd(copyFQN string) {
	delete(lom.md.copies, copyFQN)
	if len(lom.md.copies) <= 1 {
		lom.md.copies = nil
	}
}

func (lom *LOM) AddCopy(copyFQN string, mpi *fs.MountpathInfo) error {
	lom.addCopyMd(copyFQN, mpi)
	return lom.syncMetaWithCopies()
}

func (lom *LOM) DelCopies(copiesFQN ...string) (err error) {
	numCopies := lom.NumCopies()
	// 1. Delete all copies from metadata
	for _, copyFQN := range copiesFQN {
		if _, ok := lom.md.copies[copyFQN]; !ok {
			return fmt.Errorf("lom %s(num: %d): copy %s %s", lom, numCopies, copyFQN, cmn.DoesNotExist)
		}
		lom.delCopyMd(copyFQN)
	}

	// 2. Try to update metadata on left copies
	if err := lom.syncMetaWithCopies(); err != nil {
		return err // Hard error which probably removed default object.
	}

	// 3. If everything succeeded, finally remove the requested copies.
	//
	// NOTE: We should not report error if there was problem with removing copies.
	// LRU should take care of that later.
	for _, copyFQN := range copiesFQN {
		if err1 := cmn.RemoveFile(copyFQN); err1 != nil {
			glog.Error(err1)
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
func (lom *LOM) DelExtraCopies() (err error) {
	if lom._whingeCopy() {
		return
	}
	availablePaths, _ := fs.Get()
	for _, mpathInfo := range availablePaths {
		copyFQN := fs.CSM.FQN(mpathInfo, lom.bck.Bck, lom.ParsedFQN.ContentType, lom.ObjName)
		if _, ok := lom.md.copies[copyFQN]; ok {
			continue
		}
		if err1 := cmn.RemoveFile(copyFQN); err1 != nil {
			err = err1
			continue
		}
	}
	return
}

// syncMetaWithCopies tries to make sure that all copies have identical metadata.
// NOTE: uname for LOM must be already locked.
func (lom *LOM) syncMetaWithCopies() (err error) {
	var copyFQN string
	if !lom.IsHRW() || !lom.HasCopies() { // must have copies and be at the default location
		return nil
	}
	for {
		if copyFQN, err = lom.persistMdOnCopies(); err == nil {
			break
		}
		lom.delCopyMd(copyFQN)
		if err1 := fs.Access(copyFQN); err1 != nil && !os.IsNotExist(err1) {
			lom.T.FSHC(err, copyFQN) // TODO: notify scrubber
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
	buf, slab := lom.T.GetMMSA().Alloc()
	for path, mpathInfo := range availablePaths {
		if path == lom.ParsedFQN.MpathInfo.Path {
			continue
		}
		fqn := fs.CSM.FQN(mpathInfo, lom.bck.Bck, lom.ParsedFQN.ContentType, lom.ObjName)
		if _, err := os.Stat(fqn); err != nil {
			continue
		}
		src := lom.Clone(fqn)
		if err := src.Init(lom.bck.Bck, lom.Config()); err != nil {
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
		workFQN   = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
		srcCksum  = lom.Cksum()
		cksumType = cmn.ChecksumNone
	)
	if srcCksum != nil {
		cksumType = srcCksum.Type()
	}
	_, dstCksum, err = cmn.CopyFile(lom.FQN, workFQN, buf, cksumType)
	if err != nil {
		return
	}

	dst = lom.Clone(dstFQN)
	if err = dst.Init(cmn.Bck{}, lom.Config()); err != nil {
		return
	}
	dst.CopyMetadata(lom)

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
	if err = dst.Persist(); err != nil {
		if errRemove := os.Remove(dst.FQN); errRemove != nil {
			glog.Errorf("nested err: %v", errRemove)
		}
		return
	}
	if lom.MirrorConf().Enabled && lom.Bck().Equal(dst.Bck(), true /* must have same BID*/) {
		if err = lom.AddCopy(dst.FQN, dst.ParsedFQN.MpathInfo); err != nil {
			return
		}
		err = lom.Persist()
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
		s = fmt.Sprintf("o[%s/%s fs=%s", b, lom.ObjName, lom.ParsedFQN.MpathInfo.FileSystem)
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
	} else {
		if !lom.IsHRW() {
			a += "(misplaced)"
		}
		if lom.IsCopy() {
			a += "(copy)"
		}
		if n := lom.NumCopies(); n > 1 {
			a += fmt.Sprintf("(%dc)", n)
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
func (lom *LOM) LoadBalanceGET(now time.Time) (fqn string) {
	if !lom.HasCopies() {
		return lom.FQN
	}
	return fs.LoadBalanceGET(lom.FQN, lom.ParsedFQN.MpathInfo.Path, lom.GetCopies(), now)
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
		lom.Uncache()
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
	if lom.md.cksum != nil && lom.md.cksum.Type() != cmn.ChecksumNone {
		cksumType = lom.md.cksum.Type() // takes precedence on the other hand
	}
	if cksums.comp, err = lom.ComputeCksum(cksumType); err != nil {
		return
	}
	if lom.md.cksum == nil || lom.md.cksum.Type() == cmn.ChecksumNone { // store computed
		lom.md.cksum = cksums.comp.Clone()
		if err = lom.Persist(); err != nil {
			lom.md.cksum = cksums.stor
			return
		}
		lom.ReCache()
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
	lom.Uncache()
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
	buf, slab := lom.T.GetMMSA().Alloc(lom.Size())
	_, cksum, err = cmn.CopyAndChecksum(ioutil.Discard, file, buf, cksumType)
	debug.AssertNoErr(file.Close())
	slab.Free(buf)
	if err != nil {
		return nil, err
	}
	return
}

// NOTE: Clone performs shallow copy of the LOM struct.
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
	debug.Assert(lom.ParsedFQN.Digest != 0)
	return lom.md.uname, int(lom.ParsedFQN.Digest & (cmn.MultiSyncMapCount - 1))
}

func (lom *LOM) Init(bck cmn.Bck, config ...*cmn.Config) (err error) {
	if lom.FQN != "" {
		lom.ParsedFQN, lom.HrwFQN, err = ResolveFQN(lom.FQN)
		if err != nil {
			return
		}
		if bck.Name == "" {
			bck.Name = lom.ParsedFQN.Bck.Name
		} else if bck.Name != lom.ParsedFQN.Bck.Name {
			return fmt.Errorf("lom-init %s: bucket mismatch (%s != %s)", lom.FQN, bck, lom.ParsedFQN.Bck)
		}
		lom.ObjName = lom.ParsedFQN.ObjName
		prov := lom.ParsedFQN.Bck.Provider
		if bck.Provider == "" {
			bck.Provider = prov
		} else if bck.Provider != prov {
			return fmt.Errorf("lom-init %s: provider mismatch (%s != %s)", lom.FQN, bck.Provider, prov)
		}
		if bck.Ns.IsGlobal() {
			bck.Ns = lom.ParsedFQN.Bck.Ns
		} else if bck.Ns != lom.ParsedFQN.Bck.Ns {
			return fmt.Errorf("lom-init %s: namespace mismatch (%s != %s)", lom.FQN, bck.Ns, lom.ParsedFQN.Bck.Ns)
		}
	}
	bowner := lom.T.GetBowner()
	lom.bck = NewBckEmbed(bck)
	if err = lom.bck.Init(bowner, lom.T.Snode()); err != nil {
		return
	}
	lom.md.uname = lom.bck.MakeUname(lom.ObjName)
	if lom.FQN == "" {
		lom.ParsedFQN.MpathInfo, lom.ParsedFQN.Digest, err = HrwMpath(lom.md.uname)
		if err != nil {
			return
		}
		lom.ParsedFQN.ContentType = fs.ObjectType
		lom.FQN = fs.CSM.FQN(lom.ParsedFQN.MpathInfo, lom.bck.Bck, fs.ObjectType, lom.ObjName)
		lom.HrwFQN = lom.FQN
		lom.ParsedFQN.Bck = lom.bck.Bck
		lom.ParsedFQN.ObjName = lom.ObjName
	}
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
	_, ok = cache.Load(hkey)
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
	debug.Assert(lom.loaded) // cannot check bucket without first calling lom.Load()
	var (
		bmd             = lom.T.GetBowner().Get()
		bprops, present = bmd.Get(lom.bck)
	)
	if !present { // bucket does not exist
		node := lom.T.Snode().String()
		if lom.bck.IsRemote() {
			return cmn.NewErrorRemoteBucketDoesNotExist(lom.Bck().Bck, node)
		}
		return cmn.NewErrorBucketDoesNotExist(lom.Bck().Bck, node)
	}
	if lom.md.bckID == bprops.BID {
		return nil // ok
	}
	lom.Uncache()
	return cmn.NewObjDefunctError(lom.String(), lom.md.bckID, lom.bck.Props.BID)
}

func (lom *LOM) ReCache() {
	debug.Assert(!lom.IsCopy()) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
		md        = &lmeta{}
	)
	*md = lom.md
	md.bckID = lom.Bprops().BID
	if md.bckID != 0 {
		cache.Store(hkey, md)
	}
}

func (lom *LOM) Uncache() {
	debug.Assert(!lom.IsCopy()) // not caching copies
	var (
		hkey, idx = lom.Hkey()
		cache     = lom.ParsedFQN.MpathInfo.LomCache(idx)
	)
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
			lom.T.FSHC(err, lom.FQN)
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
		lom.T.FSHC(err, lom.FQN)
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
	lom.Uncache()
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
		go func(cache *sync.Map) {
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
					if lom, bucketExists := lomFromLmeta(md, bmd); bucketExists {
						lom.flushAtime(atime)
					}
					// TODO: throttle via mountpath.IsIdle()
				}
				cache.Delete(hkey)
				evicted.Add(1)
				return true
			}
			cache.Range(feviat)
			wg.Done()
		}(cache)
	}
	wg.Wait()
	return int(evicted.Load()), int(total.Load())
}

func LomCacheHousekeep(mem *memsys.MMSA, t Target) (housekeep hk.CleanupFunc, initialInterval time.Duration) {
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
		mi  = lom.ParsedFQN.MpathInfo
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
