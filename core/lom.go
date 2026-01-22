// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"io"
	"net/http"
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
		flags   uint64 // reserve (storage-class, compression/encryption, write-back, etc.)
	}
	LOM struct {
		mi      *fs.Mountpath
		bck     meta.Bck
		ObjName string
		FQN     string
		md      lmeta  // on-disk metadata
		digest  uint64 // uname digest
		isHRW   bool
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

func (lom *LOM) loaded() bool { return lom.md.lid != 0 } // internal

func (lom *LOM) IsHRW() bool { return lom.isHRW }

// given an existing (on-disk) object, determines whether it is a _copy_
// (compare with isMirror below)
func (lom *LOM) IsCopy() bool {
	if lom.IsHRW() {
		return false
	}
	_, ok := lom.md.copies[lom.FQN] // (compare w/ lom.haveMpath())
	return ok
}

// low-level access to the os.FileInfo of a chunk or whole file
func (lom *LOM) Fstat(getAtime bool) (size, atimefs int64, mtime time.Time, _ error) {
	finfo, err := os.Lstat(lom.FQN)
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

func (lom *LOM) Uname() string     { return *lom.md.uname }
func (lom *LOM) UnamePtr() *string { return lom.md.uname }
func (lom *LOM) Digest() uint64    { return lom.digest }

func (lom *LOM) SetSize(size int64) { lom.md.Size = size }

func (lom *LOM) Checksum() *cos.Cksum { return lom.md.Cksum }
func (lom *LOM) EqCksum(cksum *cos.Cksum) bool {
	return !cos.NoneC(lom.md.Cksum) && lom.md.Cksum.Equal(cksum)
}

func (lom *LOM) SetCksum(cksum *cos.Cksum) {
	if !cos.NoneC(cksum) {
		debug.AssertNoErr(cksum.Validate())
	}
	lom.md.Cksum = cksum
}

func (lom *LOM) Atime() time.Time      { return time.Unix(0, lom.md.Atime) }
func (lom *LOM) AtimeUnix() int64      { return lom.md.Atime }
func (lom *LOM) SetAtimeUnix(tu int64) { lom.md.Atime = tu }

// Object metadata normalization rules: Last-Modified and ETag
// The following rules define how AIS derives HTTP-visible object metadata
// (Last-Modified and ETag) in a consistent way across:
//   - cloud buckets
//   - AIS buckets
//   - objects with or without checksums
// --------------------
// Last-Modified
// --------------------
// 1) If object custom metadata contains `cos.HdrLastModified`
//    (typically populated from a cloud backend), it ALWAYS takes precedence.
//    The value is expected to be RFC1123 / http.TimeFormat (GMT), as required
//    by HTTP and S3 semantics.
// 2) Otherwise, Last-Modified is derived from the local filesystem mtime
//    (via syscall) and formatted as http.TimeFormat.
// Notes:
// - This keeps HTTP semantics correct (RFC 9110).
// - List-objects uses cmn.LsoLastModified instead, which is RFC3339 and
//   intentionally separate.
// - atime is NOT used for HTTP Last-Modified.
// --------------------
// ETag
// --------------------
//   * Internally (custom metadata, ObjAttrs):
//       - ETag is always stored unquoted.
//   * Externally (HTTP headers, S3 XML responses):
//       - ETag is always quoted.
// Precedence and generation rules:
// 1) If custom metadata contains ETag (typically from a cloud backend),
//    it always wins and is returned as is (unquoted internally).
// 2) Otherwise, if the object has an MD5 checksum, the MD5 value is used
//    as the ETag (still unquoted).
// 3) Otherwise, if the object has a non-MD5 checksum, a deterministic,
//    collision-resistant synthetic ETag is generated:
//        "v1-<checksum>-<mtime>"
//    where:
//      - <checksum> is the stored checksum value
//      - <mtime> is filesystem mtime (via syscall) encoded in base-36
// Notes:
// - ETag is treated as an opaque identifier, per S3 specification.
// - No attempt is made to preserve MD5 semantics unless MD5 actually exists.
// - Objects explicitly configured to have no checksums intentionally
//   do not receive an ETag.
// and finally:
// - Users who require strict S3-compatible semantics (for example, when
//   using S3 SDKs such as boto3) are strongly encouraged to configure
//   MD5 checksums at the bucket level.

func (lom *LOM) LastModified() (time.Time, error) {
	if s, ok := lom.GetCustomKey(cos.HdrLastModified); ok {
		if mtime, err := time.Parse(http.TimeFormat, s); err == nil { // GMT/UTC
			return mtime, nil
		}
	}
	// (note: syscall)
	return fs.MtimeUTC(lom.FQN)
}

// (same as above when callers want a string)
func (lom *LOM) LastModifiedStr() (string, time.Time) {
	if v, ok := lom.GetCustomKey(cos.HdrLastModified); ok {
		debug.AssertFunc(func() bool {
			_, err := time.Parse(http.TimeFormat, v)
			return err == nil
		})
		return v, time.Time{}
	}
	// (note: syscall)
	mtime, err := fs.MtimeUTC(lom.FQN)
	if err != nil {
		nlog.Errorln(lom.Cname(), "unexpected failure to get last-modified:", err)
		return "", time.Time{}
	}
	return mtime.Format(http.TimeFormat), mtime
}

// NOTE:
//   - (list-objects, S3): prefer stored RFC3339 (cmn.LsoLastModified);
//   - else convert Last-Modified (http.TimeFormat) to RFC3339;
//   - else (perf) fall back to atime when available
//   - clients requiring strict S3 semantics with ais:// buckets must use HEAD(object)
//     (which is allowed to execute syscalls)
func (lom *LOM) LastModifiedLso() (string, time.Time) {
	if s, ok := lom.GetCustomKey(cmn.LsoLastModified); ok {
		mtime, _ := time.Parse(time.RFC3339, s)
		return s, mtime
	}
	if s, ok := lom.GetCustomKey(cos.HdrLastModified); ok {
		if mtime, err := time.Parse(http.TimeFormat, s); err == nil { // GMT/UTC
			return mtime.Format(time.RFC3339), mtime
		}
	}
	if lom.AtimeUnix() != 0 {
		atime := lom.Atime().UTC()
		// making an exception but only for atime string
		return atime.Format(time.RFC3339), time.Time{}
	}
	return "", time.Time{}
}

// allowGen=false: return only existing (custom or MD5) values
// allowGen=true:  as the name implies
func (lom *LOM) ETag(mtime time.Time, allowSyscall bool) string {
	// 1. ETag via custom
	if etag, ok := lom.GetCustomKey(cmn.ETag); ok {
		debug.Assert(etag != "" && etag[0] != '"')
		return etag
	}

	cksum := lom.Checksum()
	if cos.NoneC(cksum) {
		return "" // (no checksum => no ETag)
	}

	// 2. MD5
	if !lom.IsChunked() && cksum.Ty() == cos.ChecksumMD5 {
		debug.Assert(cksum.Val()[0] != '"', cksum.Val())
		return cksum.Val()
	}

	// 3. mtime
	if mtime.IsZero() {
		if mtimeStr, ok := lom.GetCustomKey(cos.HdrLastModified); ok {
			tm, err := time.Parse(http.TimeFormat, mtimeStr)
			if err == nil {
				mtime = tm
			}
		}
		if mtime.IsZero() {
			if !allowSyscall {
				return ""
			}
			debug.Assert(lom.bck.RemoteBck() == nil, "must consistently use remote mtime")
			t, err := fs.MtimeUTC(lom.FQN)
			if err != nil {
				return ""
			}
			mtime = t
		}
	}

	// 4. make ETag
	var sb cos.SB
	sb.Init(len(lom.md.Cksum.Val()) + 24)
	sb.WriteString("v1-")
	sb.WriteString(lom.md.Cksum.Val())
	sb.WriteUint8('-')
	sb.WriteString(strconv.FormatInt(mtime.UnixNano(), 36))

	return sb.String()
}

//
// BID, `lomBID` (type), and the two MetaverLOM_V1 `lomFlags`
//

func (lom *LOM) bid() uint64             { return lom.md.lid.bid() }
func (lom *LOM) setbid(bpropsBID uint64) { lom.md.lid = lom.md.lid.setbid(bpropsBID) }
func (lom *LOM) setlmfl(fl lomFlags)     { lom.md.lid = lom.md.lid.setlmfl(fl) }
func (lom *LOM) clrlmfl(fl lomFlags)     { lom.md.lid = lom.md.lid.clrlmfl(fl) }

func (lom *LOM) IsChunked(special ...bool) bool { // same convention as Lsize
	debug.Assert(len(special) > 0 || lom.loaded(), lom.String())
	return lom.md.lid.haslmfl(lmflChunk)
}

func (lom *LOM) IsFntl() bool { return lom.md.lid.haslmfl(lmflFntl) }

// custom metadata
func (lom *LOM) GetCustomMD() cos.StrKVs   { return lom.md.GetCustomMD() }
func (lom *LOM) SetCustomMD(md cos.StrKVs) { lom.md.SetCustomMD(md) }

func (lom *LOM) GetCustomKey(key string) (string, bool) { return lom.md.GetCustomKey(key) }
func (lom *LOM) SetCustomKey(key, value string)         { lom.md.SetCustomKey(key, value) }
func (lom *LOM) DelCustomKey(key string)                { lom.md.DelCustomKey(key) }

// assorted _convenient_ accessors
func (lom *LOM) Bck() *meta.Bck                 { return &lom.bck }
func (lom *LOM) Bprops() *cmn.Bprops            { return lom.bck.Props }
func (lom *LOM) ECEnabled() bool                { return lom.Bprops().EC.Enabled }
func (lom *LOM) IsFeatureSet(f feat.Flags) bool { return lom.Bprops().Features.IsSet(f) }
func (lom *LOM) MirrorConf() *cmn.MirrorConf    { return &lom.Bprops().Mirror }
func (lom *LOM) VersionConf() cmn.VersionConf   { return lom.bck.VersionConf() }
func (lom *LOM) CksumConf() *cmn.CksumConf      { return lom.bck.CksumConf() }

// more cksum conf
func (lom *LOM) CksumType() string {
	c := lom.bck.CksumConf()
	if c.Type == "" || c.Type == cos.ChecksumNone {
		return cos.ChecksumNone
	}
	return c.Type
}
func (lom *LOM) ValidateWarmGet() bool {
	return lom.CksumType() != cos.ChecksumNone && lom.CksumConf().ValidateWarmGet
}
func (lom *LOM) ValidateColdGet() bool {
	return lom.CksumType() != cos.ChecksumNone && lom.CksumConf().ValidateColdGet
}

// to report via list-objects and HEAD()
func (lom *LOM) Location() string { return T.String() + apc.LocationPropSepa + lom.mi.String() }

// as fs.PartsFQN
func (lom *LOM) ObjectName() string       { return lom.ObjName }
func (lom *LOM) Bucket() *cmn.Bck         { return (*cmn.Bck)(&lom.bck) }
func (lom *LOM) Mountpath() *fs.Mountpath { return lom.mi }

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

func (lom *LOM) HrwTarget(smap *meta.Smap) (tsi *meta.Snode, local bool, err error) {
	tsi, err = smap.HrwHash2T(lom.digest)
	if err != nil {
		return
	}
	local = tsi.ID() == T.SID()
	return
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
	md, err = lom.lmfsReload(false /*populate*/)
	if err != nil {
		goto rerr
	}
	if md == nil {
		err = fmt.Errorf("%s: no meta", lom)
		goto rerr
	}
	if err = md.Cksum.Validate(); err != nil {
		context := fmt.Sprintf("%q metadata corruption: %v", lom, err)
		err = cos.NewErrDataCksum(lom.md.Cksum, md.Cksum, context)
		goto rerr
	}
	if lom.md.Cksum == nil {
		lom.SetCksum(md.Cksum)
		return nil
	}
	// different versions may have different checksums
	if md.Version() == lom.md.Version() && !lom.EqCksum(md.Cksum) {
		err = cos.NewErrDataCksum(lom.md.Cksum, md.Cksum, lom.String())
		goto rerr
	}
	return nil
rerr:
	lom.UncacheDel()
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
	if !cos.NoneC(lom.md.Cksum) {
		cksumType = lom.md.Cksum.Ty() // takes precedence on the other hand
	}
	if cksums.comp, err = lom.ComputeCksum(cksumType, locked); err != nil {
		return err
	}
	if cos.NoneC(lom.md.Cksum) { // store computed
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
		defer lom.Unlock(false)
	}
	if err := lom.Load(false, true); err != nil {
		return nil, fmt.Errorf("compute-checksum -- load: %v", err)
	}
	lmfh, err := lom.Open()
	if err != nil {
		return nil, fmt.Errorf("compute-checksum -- open: %v", err)
	}

	// `buf` is nil: `io.Discard` has efficient `io.ReaderFrom`
	_, cksum, err = cos.CopyAndChecksum(io.Discard, lmfh, nil, cksumType)
	cos.Close(lmfh)
	return cksum, err
}

// no lock is taken when locked by an immediate caller, or otherwise is known to be locked
// otherwise, try Rlock temporarily _if and only when_ reading from fs
//
// (compare w/ LoadUnsafe() below)
func (lom *LOM) Load(cacheit, locked bool) error {
	debug.Assert(lom.Bprops() != nil, lom.Cname()) // must be InitBck/InitFQN'ed
	var (
		lcache, lmd = lom.fromCache()
	)
	// fast path
	if lmd != nil {
		lom.md = *lmd
		if lom.IsFntl() {
			lom.fixupFntl()
		}
		err := lom._checkBucket()
		if !cos.IsNotExist(err) {
			return err
		}
	}

	// slow path
	if !locked && lom.TryLock(false) {
		defer lom.Unlock(false)
	}
	if err := lom.FromFS(); err != nil {
		return err
	}

	if lom.bid() == 0 { // when LOM is a _handle_
		lom.setbid(lom.Bprops().BID)
	}
	if err := lom._checkBucket(); err != nil {
		return err
	}
	if cacheit {
		debug.Assert(lom.bid() != 0)
		md := lom.md
		lcache.Store(lom.digest, &md)
	}
	return nil
}

func (lom *LOM) _checkBucket() error {
	bck := &lom.bck
	bmd := T.Bowner().Get()
	bprops, present := bmd.Get(bck)
	if !present {
		lom.UncacheDel()
		return cmn.NewErrBckNotFound(bck.Bucket())
	}
	if lom.bid() == bprops.BID {
		debug.Assert(bprops.BID != 0)
		return nil
	}
	err := cmn.NewErrObjDefunct(lom.String(), lom.bid(), bprops.BID)
	if cmn.Rom.V(4, cos.ModCore) {
		nlog.Warningln(err)
	}
	lom.UncacheDel()
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
	if cmn.Rom.V(4, cos.ModCore) || lom.digest&0xf == 5 {
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
		lom.setbid(lom.Bprops().BID)
		lom.setlmfl(lmflFntl)

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
		lom.clrlmfl(lmflFntl)
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
	if !lom.md.lid.haslmfl(lmflChunk) {
		if lom.md.Size != size { // corruption or tampering
			return cmn.NewErrLmetaCorrupted(lom.whingeSize(size))
		}
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

// (compare with fs/content Gen())
func (lom *LOM) ShortenFntl() []string {
	debug.Assert(fs.IsFntl(lom.ObjName), lom.FQN)

	noname := fs.ShortenFntl(lom.FQN)
	nfqn := lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, noname)

	debug.Assert(len(nfqn) < 4096, "PATH_MAX /usr/include/limits.h", len(nfqn))
	return []string{nfqn, noname}
}

// TODO -- FIXME: revisit metaver v2 usage
func (lom *LOM) fixupFntl() {
	if !fs.IsFntl(lom.ObjName) {
		return
	}
	lom.ObjName = fs.ShortenFntl(lom.FQN)                             // noname
	lom.FQN = lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName) // nfqn
	lom.isHRW = true
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
	return saved
}

func (lom *LOM) PopFntl(saved []string) {
	lom.FQN, lom.ObjName = saved[0], saved[1]
}
