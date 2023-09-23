// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	ratomic "sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ios"
	"github.com/OneOfOne/xxhash"
)

const bidUnknownTTL = 2 * time.Minute // comment below; TODO: unify and move to config along w/ lom cache

const nodeXattrID = "user.ais.daemon_id"

// enum Mountpath.Flags
const (
	FlagBeingDisabled uint64 = 1 << iota
	FlagBeingDetached
)

const FlagWaitingDD = FlagBeingDisabled | FlagBeingDetached

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

type (
	Mountpath struct {
		lomCaches  cos.MultiSyncMap // LOM caches
		info       string
		Path       string   // clean path
		cos.FS              // underlying filesystem
		Disks      []string // owned disks (ios.FsDisks map => slice)
		flags      uint64   // bit flags (set/get atomic)
		PathDigest uint64   // (HRW logic)
		capacity   Capacity
	}
	MPI map[string]*Mountpath

	// MountedFS holds all mountpaths for the target.
	MountedFS struct {
		// Iostats for the available mountpaths
		ios ios.IOS

		// fsIDs is set in which we store fsids of mountpaths. This allows for
		// determining if there are any duplications of file system - we allow
		// only one mountpath per file system.
		fsIDs map[cos.FsID]string

		// mountpaths
		available ratomic.Pointer[MPI]
		disabled  ratomic.Pointer[MPI]

		// capacity
		cs        CapStatus
		csExpires atomic.Int64

		mu sync.RWMutex

		// allow disk sharing by multiple mountpaths and mountpaths with no disks whatsoever
		// (default = false)
		allowSharedDisksAndNoDisks bool
	}
	CapStatus struct {
		// config
		HighWM int64
		OOS    int64
		// metrics
		TotalUsed  uint64 // bytes
		TotalAvail uint64 // bytes
		PctAvg     int32  // average used (%)
		PctMax     int32  // max used (%)
		PctMin     int32  // max used (%)
	}
)

var mfs *MountedFS

///////////////
// Mountpath //
///////////////

func NewMountpath(mpath string) (mi *Mountpath, err error) {
	var (
		cleanMpath string
		fsInfo     cos.FS
	)
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	if err = cos.Stat(cleanMpath); err != nil {
		return nil, cos.NewErrNotFound("mountpath %q", mpath)
	}
	if fsInfo, err = makeFsInfo(cleanMpath); err != nil {
		return
	}
	mi = &Mountpath{
		Path:       cleanMpath,
		FS:         fsInfo,
		PathDigest: xxhash.ChecksumString64S(cleanMpath, cos.MLCG32),
	}
	return
}

// flags
func (mi *Mountpath) setFlags(flags uint64) (ok bool) {
	return cos.SetfAtomic(&mi.flags, flags)
}

func (mi *Mountpath) IsAnySet(flags uint64) bool {
	return cos.IsAnySetfAtomic(&mi.flags, flags)
}

func (mi *Mountpath) String() string {
	if mi.info == "" {
		switch len(mi.Disks) {
		case 0:
			mi.info = fmt.Sprintf("mp[%s, fs=%s]", mi.Path, mi.Fs)
		case 1:
			mi.info = fmt.Sprintf("mp[%s, %s]", mi.Path, mi.Disks[0])
		default:
			mi.info = fmt.Sprintf("mp[%s, %v]", mi.Path, mi.Disks)
		}
	}
	if !mi.IsAnySet(FlagWaitingDD) {
		return mi.info
	}
	l := len(mi.info)
	return mi.info[:l-1] + ", waiting-dd]"
}

func (mi *Mountpath) LomCache(idx int) *sync.Map { return mi.lomCaches.Get(idx) }

func LcacheIdx(digest uint64) int { return int(digest & (cos.MultiSyncMapCount - 1)) }

func (mi *Mountpath) EvictLomCache() {
	for idx := 0; idx < cos.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(key any, _ any) bool {
			cache.Delete(key)
			return true
		})
	}
}

func (mi *Mountpath) evictLomBucketCache(bck *cmn.Bck) {
	for idx := 0; idx < cos.MultiSyncMapCount; idx++ {
		cache := mi.LomCache(idx)
		cache.Range(func(hkey any, _ any) bool {
			uname := hkey.(string)
			b, _ := cmn.ParseUname(uname)
			if b.Equal(bck) {
				cache.Delete(hkey)
			}
			return true
		})
	}
}

func (mi *Mountpath) IsIdle(config *cmn.Config) bool {
	curr := mfs.ios.GetMpathUtil(mi.Path)
	return curr >= 0 && curr < config.Disk.DiskUtilLowWM
}

func (mi *Mountpath) CreateMissingBckDirs(bck *cmn.Bck) (err error) {
	for contentType := range CSM.m {
		dir := mi.MakePathCT(bck, contentType)
		if err = cos.Stat(dir); err == nil {
			continue
		}
		if err = cos.CreateDir(dir); err != nil {
			return
		}
	}
	return
}

func (mi *Mountpath) backupAtmost(from, backup string, bcnt, atMost int) (newBcnt int) {
	var (
		fromPath   = filepath.Join(mi.Path, from)
		backupPath = filepath.Join(mi.Path, backup)
	)
	os.Remove(backupPath)
	newBcnt = bcnt
	if bcnt >= atMost {
		return
	}
	if err := cos.Stat(fromPath); err != nil {
		return
	}
	if err := os.Rename(fromPath, backupPath); err != nil {
		nlog.Errorln(err)
		os.Remove(fromPath)
	} else {
		newBcnt = bcnt + 1
	}
	return
}

func (mi *Mountpath) SetDaemonIDXattr(tid string) error {
	cos.Assert(tid != "")
	// Validate if mountpath already has daemon ID set.
	mpathDaeID, err := _loadXattrID(mi.Path)
	if err != nil {
		return err
	}
	if mpathDaeID == tid {
		return nil
	}
	if mpathDaeID != "" && mpathDaeID != tid {
		return &ErrStorageIntegrity{
			Code: SieMpathIDMismatch,
			Msg:  fmt.Sprintf("target ID mismatch: %q vs %q(%q)", tid, mpathDaeID, mi),
		}
	}
	return SetXattr(mi.Path, nodeXattrID, []byte(tid))
}

// make-path methods

func (mi *Mountpath) makePathBuf(bck *cmn.Bck, contentType string, extra int) (buf []byte) {
	var provLen, nsLen, bckNameLen, ctLen int
	if contentType != "" {
		debug.Assert(len(contentType) == contentTypeLen)
		debug.Assert(bck.Props == nil || bck.Props.BID != 0)
		ctLen = 1 + 1 + contentTypeLen
	}
	if !bck.Ns.IsGlobal() {
		nsLen = 1
		if bck.Ns.IsRemote() {
			nsLen += 1 + len(bck.Ns.UUID)
		}
		nsLen += 1 + len(bck.Ns.Name)
	}
	if bck.Name != "" {
		bckNameLen = 1 + len(bck.Name)
	}
	provLen = 1 + 1 + len(bck.Provider)
	buf = make([]byte, 0, len(mi.Path)+provLen+nsLen+bckNameLen+ctLen+extra)
	buf = append(buf, mi.Path...)
	buf = append(buf, filepath.Separator, prefProvider)
	buf = append(buf, bck.Provider...)
	if nsLen > 0 {
		buf = append(buf, filepath.Separator)
		if bck.Ns.IsRemote() {
			buf = append(buf, prefNsUUID)
			buf = append(buf, bck.Ns.UUID...)
		}
		buf = append(buf, prefNsName)
		buf = append(buf, bck.Ns.Name...)
	}
	if bckNameLen > 0 {
		buf = append(buf, filepath.Separator)
		buf = append(buf, bck.Name...)
	}
	if ctLen > 0 {
		buf = append(buf, filepath.Separator, prefCT)
		buf = append(buf, contentType...)
	}
	return
}

func (mi *Mountpath) MakePathBck(bck *cmn.Bck) string {
	buf := mi.makePathBuf(bck, "", 0)
	return cos.UnsafeS(buf)
}

func (mi *Mountpath) MakePathCT(bck *cmn.Bck, contentType string) string {
	debug.Assert(contentType != "")
	buf := mi.makePathBuf(bck, contentType, 0)
	return cos.UnsafeS(buf)
}

func (mi *Mountpath) MakePathFQN(bck *cmn.Bck, contentType, objName string) string {
	debug.Assert(contentType != "" && objName != "")
	buf := mi.makePathBuf(bck, contentType, 1+len(objName))
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return cos.UnsafeS(buf)
}

func (mi *Mountpath) makeDelPathBck(bck *cmn.Bck) string {
	return mi.MakePathBck(bck)
}

// Creates all CT directories for a given (mountpath, bck) - NOTE handling of empty dirs
func (mi *Mountpath) createBckDirs(bck *cmn.Bck, nilbmd bool) (int, error) {
	var num int
	for contentType := range CSM.m {
		dir := mi.MakePathCT(bck, contentType)
		if err := cos.Stat(dir); err == nil {
			if nilbmd {
				// a) loaded previous BMD version or b) failed to load any
				// in both cases, BMD cannot be fully trusted, and so we ignore that fact
				// that the directory exists
				// (scenario: decommission without proper cleanup, followed by rejoin)
				nlog.Errorf("Warning: %s bdir %s exists but local BMD is not the latest", bck, dir)
				num++
				continue
			}
			names, empty, errEmpty := IsDirEmpty(dir)
			if errEmpty != nil {
				return num, errEmpty
			}
			if !empty {
				err = fmt.Errorf("bucket %s: directory %s already exists and is not empty (%v...)",
					bck, dir, names)
				if contentType != WorkfileType {
					return num, err
				}
				nlog.Errorln(err)
			}
		} else if err := cos.CreateDir(dir); err != nil {
			return num, fmt.Errorf("bucket %s: failed to create directory %s: %w", bck, dir, err)
		}
		num++
	}
	return num, nil
}

func (mi *Mountpath) _setDisks(fsdisks ios.FsDisks) {
	mi.Disks = make([]string, len(fsdisks))
	i := 0
	for d := range fsdisks {
		mi.Disks[i] = d
		i++
	}
}

// available/used capacity

func (mi *Mountpath) getCapacity(config *cmn.Config, refresh bool) (c Capacity, err error) {
	if !refresh {
		c.Used = ratomic.LoadUint64(&mi.capacity.Used)
		c.Avail = ratomic.LoadUint64(&mi.capacity.Avail)
		c.PctUsed = ratomic.LoadInt32(&mi.capacity.PctUsed)
		return
	}
	statfs := &syscall.Statfs_t{}
	if err = syscall.Statfs(mi.Path, statfs); err != nil {
		return
	}
	bused := statfs.Blocks - statfs.Bavail
	pct := bused * 100 / statfs.Blocks
	if pct >= uint64(config.Space.HighWM)-1 {
		fpct := math.Ceil(float64(bused) * 100 / float64(statfs.Blocks))
		pct = uint64(fpct)
	}
	u := bused * uint64(statfs.Bsize)
	ratomic.StoreUint64(&mi.capacity.Used, u)
	c.Used = u
	a := statfs.Bavail * uint64(statfs.Bsize)
	ratomic.StoreUint64(&mi.capacity.Avail, a)
	c.Avail = a
	ratomic.StoreInt32(&mi.capacity.PctUsed, int32(pct))
	c.PctUsed = int32(pct)
	return
}

//
// mountpath add/enable helpers - always call under mfs lock
//

func (mi *Mountpath) AddEnabled(tid string, avail MPI, config *cmn.Config) (err error) {
	if err = mi._checkExists(avail); err != nil {
		return
	}
	if err = mi._addEnabled(tid, avail, config); err == nil {
		mfs.fsIDs[mi.FsID] = mi.Path
	}
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
	return
}

func (mi *Mountpath) AddDisabled(disabled MPI) {
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
	disabled[mi.Path] = mi
	mfs.fsIDs[mi.FsID] = mi.Path
}

// TODO: extend `force=true` to disregard "filesystem sharing" (see AddMpath)
func (mi *Mountpath) _checkExists(avail MPI) (err error) {
	if existingMi, exists := avail[mi.Path]; exists {
		err = fmt.Errorf("failed adding %s: %s already exists", mi, existingMi)
	} else if existingPath, exists := mfs.fsIDs[mi.FsID]; exists && !mfs.allowSharedDisksAndNoDisks {
		err = fmt.Errorf("FSID %v: filesystem sharing is not allowed: %s vs %q", mi.FsID, mi, existingPath)
	} else {
		l := len(mi.Path)
		for mpath := range avail {
			if err = cmn.IsNestedMpath(mi.Path, l, mpath); err != nil {
				break
			}
		}
	}
	return
}

func (mi *Mountpath) _addEnabled(tid string, avail MPI, config *cmn.Config) error {
	disks, err := mfs.ios.AddMpath(mi.Path, mi.Fs, config.TestingEnv())
	if err != nil {
		return err
	}
	if tid != "" && config.WritePolicy.MD != apc.WriteNever {
		if err := mi.SetDaemonIDXattr(tid); err != nil {
			return err
		}
	}
	mi._setDisks(disks)
	_ = mi.String() // assign mi.info if not yet
	avail[mi.Path] = mi
	return nil
}

// under lock: clones and adds self to available
func (mi *Mountpath) _cloneAddEnabled(tid string, config *cmn.Config) (err error) {
	debug.Assert(!mi.IsAnySet(FlagWaitingDD)) // m.b. new
	avail, disabled := Get()
	if _, ok := disabled[mi.Path]; ok {
		return fmt.Errorf("%s exists and is currently disabled (hint: did you want to enable it?)", mi)
	}

	// dd-transition
	if ddmi, ok := avail[mi.Path]; ok && ddmi.IsAnySet(FlagWaitingDD) {
		availableCopy := _cloneOne(avail)
		nlog.Warningf("%s (%s): interrupting dd-transition - adding&enabling", mi, ddmi)
		availableCopy[mi.Path] = mi
		putAvailMPI(availableCopy)
		return
	}

	// add new mp
	if err = mi._checkExists(avail); err != nil {
		return
	}
	availableCopy := _cloneOne(avail)
	if err = mi.AddEnabled(tid, availableCopy, config); err == nil {
		putAvailMPI(availableCopy)
	}
	return
}

func (mi *Mountpath) CheckDisks() (err error) {
	if !mfs.allowSharedDisksAndNoDisks && len(mi.Disks) == 0 {
		err = &ErrMountpathNoDisks{Mi: mi}
	}
	return
}

func (mi *Mountpath) ClearDD() {
	cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
}

//
// MountedFS & MPI
//

// create a new singleton
func New(num int, allowSharedDisksAndNoDisks bool) {
	if allowSharedDisksAndNoDisks {
		nlog.Warningln("allowed: (I) disk sharing by multiple mountpaths and (II) mountpaths with no disks")
	}
	mfs = &MountedFS{fsIDs: make(map[cos.FsID]string, 10), allowSharedDisksAndNoDisks: allowSharedDisksAndNoDisks}
	mfs.ios = ios.New(num)
}

// used only in tests
func TestNew(iostater ios.IOS) {
	const num = 10
	mfs = &MountedFS{fsIDs: make(map[cos.FsID]string, num), allowSharedDisksAndNoDisks: false}
	if iostater == nil {
		mfs.ios = ios.New(num)
	} else {
		mfs.ios = iostater
	}
	PutMPI(make(MPI, num), make(MPI, num))
}

// `ios` delegations
func Clblk()                                   { ios.Clblk(mfs.ios) }
func GetAllMpathUtils() (utils *ios.MpathUtil) { return mfs.ios.GetAllMpathUtils() }
func GetMpathUtil(mpath string) int64          { return mfs.ios.GetMpathUtil(mpath) }
func FillDiskStats(m ios.AllDiskStats)         { mfs.ios.FillDiskStats(m) }

// TestDisableValidation disables fsid checking and allows mountpaths without disks (testing-only)
func TestDisableValidation() { mfs.allowSharedDisksAndNoDisks = true }

// Returns number of available mountpaths
func NumAvail() int {
	avail := mfs.available.Load()
	return len(*avail)
}

func putAvailMPI(available MPI) { mfs.available.Store(&available) }
func putDisabMPI(disabled MPI)  { mfs.disabled.Store(&disabled) }

func PutMPI(available, disabled MPI) {
	putAvailMPI(available)
	putDisabMPI(disabled)
}

func MountpathsToLists() (mpl *apc.MountpathList) {
	avail, disabled := Get()
	mpl = &apc.MountpathList{
		Available: make([]string, 0, len(avail)),
		WaitingDD: make([]string, 0),
		Disabled:  make([]string, 0, len(disabled)),
	}
	for _, mi := range avail {
		if mi.IsAnySet(FlagWaitingDD) {
			mpl.WaitingDD = append(mpl.WaitingDD, mi.Path)
		} else {
			mpl.Available = append(mpl.Available, mi.Path)
		}
	}
	for mpath := range disabled {
		mpl.Disabled = append(mpl.Disabled, mpath)
	}
	sort.Strings(mpl.Available)
	sort.Strings(mpl.WaitingDD)
	sort.Strings(mpl.Disabled)
	return
}

// NOTE: must be under mfs lock
func _cloneOne(mpis MPI) (clone MPI) {
	clone = make(MPI, len(mpis))
	for mpath, mi := range mpis {
		clone[mpath] = mi
	}
	return
}

// cloneMPI returns a shallow copy of the current (available, disabled) mountpaths
func cloneMPI() (availableCopy, disabledCopy MPI) {
	avail, disabled := Get()
	availableCopy = _cloneOne(avail)
	disabledCopy = _cloneOne(disabled)
	return availableCopy, disabledCopy
}

// (used only in _unit_ tests - compare with AddMpath below)
func Add(mpath, tid string) (mi *Mountpath, err error) {
	mi, err = NewMountpath(mpath)
	if err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	mfs.mu.Unlock()
	return
}

// Add adds new mountpath to the target's `avail`
// TODO: extend `force=true` to disregard "filesystem sharing"
func AddMpath(mpath, tid string, cb func(), force bool) (mi *Mountpath, err error) {
	debug.Assert(tid != "")
	mi, err = NewMountpath(mpath)
	if err != nil {
		return
	}
	config := cmn.GCO.Get()
	if config.TestingEnv() {
		if err = config.LocalConfig.TestFSP.ValidateMpath(mi.Path); err != nil {
			if !force {
				return
			}
			nlog.Errorf("%v - ignoring since force=%t", err, force)
		}
	}
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	if err == nil {
		err = mi.CheckDisks()
	}
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()

	if mi.Path != mpath {
		nlog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
	}
	return
}

// (used only in tests - compare with EnableMpath below)
func Enable(mpath string) (enabledMpath *Mountpath, err error) {
	var cleanMpath string
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	enabledMpath, err = enable(mpath, cleanMpath, "" /*tid*/, config)
	mfs.mu.Unlock()
	return
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func EnableMpath(mpath, tid string, cb func()) (enabledMpath *Mountpath, err error) {
	var cleanMpath string
	debug.Assert(tid != "")
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	enabledMpath, err = enable(mpath, cleanMpath, tid, config)
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()
	return
}

func enable(mpath, cleanMpath, tid string, config *cmn.Config) (enabledMpath *Mountpath, err error) {
	avail, disabled := Get()
	mi, ok := avail[cleanMpath]

	// dd-transition
	if ok {
		debug.Assert(cleanMpath == mi.Path)
		if _, ok = disabled[cleanMpath]; ok {
			err = fmt.Errorf("FATAL: %s vs (%s, %s)", mi, avail, disabled)
			nlog.Errorln(err)
			debug.AssertNoErr(err)
			return
		}
		if mi.IsAnySet(FlagWaitingDD) {
			availableCopy := _cloneOne(avail)
			mi, ok = availableCopy[cleanMpath]
			debug.Assert(ok)
			nlog.Warningf("%s: re-enabling during dd-transition", mi)
			cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
			enabledMpath = mi
			putAvailMPI(availableCopy)
		} else if config.FastV(4, cos.SmoduleFS) {
			nlog.Infof("%s: %s is already available, nothing to do", tid, mi)
		}
		return
	}

	// re-enable
	mi, ok = disabled[cleanMpath]
	if !ok {
		err = cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
		return
	}
	debug.Assert(cleanMpath == mi.Path)
	availableCopy, disabledCopy := cloneMPI()
	mi, ok = disabledCopy[cleanMpath]
	debug.Assert(ok)
	if err = mi.AddEnabled(tid, availableCopy, config); err != nil {
		return
	}
	enabledMpath = mi
	delete(disabledCopy, cleanMpath)
	PutMPI(availableCopy, disabledCopy)
	return
}

// Remove removes mountpaths from the target's mountpaths. It searches
// for the mountpath in `available` and, if not found, in `disabled`.
func Remove(mpath string, cb ...func()) (*Mountpath, error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	// Clear target ID if set
	if err := removeXattr(cleanMpath, nodeXattrID); err != nil {
		return nil, err
	}
	avail, disabled := Get()
	mi, exists := avail[cleanMpath]
	if !exists {
		if mi, exists = disabled[cleanMpath]; !exists {
			return nil, cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
		}
		debug.Assert(cleanMpath == mi.Path)
		disabledCopy := _cloneOne(disabled)
		delete(disabledCopy, cleanMpath)
		delete(mfs.fsIDs, mi.FsID) // optional, benign
		putDisabMPI(disabledCopy)
		return mi, nil
	}
	debug.Assert(cleanMpath == mi.Path)

	if _, exists = disabled[cleanMpath]; exists {
		err := fmt.Errorf("FATAL: %s vs (%s, %s)", mi, avail, disabled)
		nlog.Errorln(err)
		debug.AssertNoErr(err)
		return nil, err
	}

	config := cmn.GCO.Get()
	availableCopy := _cloneOne(avail)
	mfs.ios.RemoveMpath(cleanMpath, config.TestingEnv())
	delete(availableCopy, cleanMpath)
	delete(mfs.fsIDs, mi.FsID)

	availCnt := len(availableCopy)
	if availCnt == 0 {
		nlog.Errorf("removed the last available mountpath %s", mi)
	} else {
		nlog.Infof("removed mountpath %s (remain available: %d)", mi, availCnt)
	}
	moveMarkers(availableCopy, mi)
	putAvailMPI(availableCopy)
	if availCnt > 0 && len(cb) > 0 {
		cb[0]()
	}
	return mi, nil
}

// begin (disable | detach) transaction: CoW-mark the corresponding mountpath
func BeginDD(action string, flags uint64, mpath string) (mi *Mountpath, numAvail int, noResil bool, err error) {
	var cleanMpath string
	debug.Assert(cos.BitFlags(flags).IsAnySet(cos.BitFlags(FlagWaitingDD)))
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	mfs.mu.Lock()
	mi, numAvail, noResil, err = begdd(action, flags, cleanMpath)
	mfs.mu.Unlock()
	return
}

// under lock
func begdd(action string, flags uint64, mpath string) (mi *Mountpath, numAvail int, noResil bool, err error) {
	var (
		avail, disabled = Get()
		exists          bool
	)
	// dd inactive
	if _, exists = avail[mpath]; !exists {
		noResil = true
		if mi, exists = disabled[mpath]; !exists {
			err = cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
			return
		}
		if action == apc.ActMountpathDisable {
			nlog.Infof("%s(%q) is already fully disabled - nothing to do", mi, action)
			mi = nil
		}
		numAvail = len(avail)
		return
	}
	// dd active
	clone := _cloneOne(avail)
	mi = clone[mpath]
	ok := mi.setFlags(flags)
	debug.Assert(ok, mi.String()) // under lock
	putAvailMPI(clone)
	numAvail = len(clone) - 1
	return
}

// Disables a mountpath, i.e., removes it from usage but keeps in the volume
// (for possible future re-enablement). If successful, returns the disabled mountpath.
// Otherwise, returns nil (also in the case if the mountpath was already disabled).
func Disable(mpath string, cb ...func()) (disabledMpath *Mountpath, err error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}

	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	avail, disabled := Get()
	if mi, ok := avail[cleanMpath]; ok {
		debug.Assert(cleanMpath == mi.Path)
		if _, ok = disabled[cleanMpath]; ok {
			err = fmt.Errorf("FATAL: %s vs (%s, %s)", mi, avail, disabled)
			nlog.Errorln(err)
			debug.AssertNoErr(err)
			return
		}
		availableCopy, disabledCopy := cloneMPI()
		cos.ClearfAtomic(&mi.flags, FlagWaitingDD)
		disabledCopy[cleanMpath] = mi

		config := cmn.GCO.Get()
		mfs.ios.RemoveMpath(cleanMpath, config.TestingEnv())
		delete(availableCopy, cleanMpath)
		delete(mfs.fsIDs, mi.FsID)
		moveMarkers(availableCopy, mi)
		PutMPI(availableCopy, disabledCopy)
		if l := len(availableCopy); l == 0 {
			nlog.Errorf("disabled the last available mountpath %s", mi)
		} else {
			if len(cb) > 0 {
				cb[0]()
			}
			nlog.Infof("disabled mountpath %s (%d remain%s active)", mi, l, cos.Plural(l))
		}
		return mi, nil
	}

	if _, ok := disabled[cleanMpath]; ok {
		return nil, nil // nothing to do
	}
	return nil, cmn.NewErrMountpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
}

// returns both available and disabled mountpaths (compare with GetAvail)
func Get() (MPI, MPI) {
	var (
		avail    = mfs.available.Load()
		disabled = mfs.disabled.Load()
	)
	debug.Assert(avail != nil)
	debug.Assert(disabled != nil)
	return *avail, *disabled
}

func GetAvail() MPI {
	avail := mfs.available.Load()
	debug.Assert(avail != nil)
	return *avail
}

func CreateBucket(bck *cmn.Bck, nilbmd bool) (errs []error) {
	var (
		avail            = GetAvail()
		totalDirs        = len(avail) * len(CSM.m)
		totalCreatedDirs int
	)
	for _, mi := range avail {
		num, err := mi.createBckDirs(bck, nilbmd)
		if err != nil {
			errs = append(errs, err)
		} else {
			totalCreatedDirs += num
		}
	}
	debug.Assert(totalCreatedDirs == totalDirs || errs != nil)
	return
}

func DestroyBucket(op string, bck *cmn.Bck, bid uint64) (err error) {
	var (
		n     int
		avail = GetAvail()
		count = len(avail)
		now   time.Time
	)
	for _, mi := range avail {
		// normally, unique bucket ID (aka BID) must be known
		// - i.e., non-zero (and unique);
		// zero ID indicates that either we are in the middle of bucket
		// creation OR the latter was interrupted (and txn-create aborted) -
		// thus, prior to going ahead with deletion:
		if bid == 0 {
			bdir := mi.MakePathBck(bck)
			if finfo, erc := os.Stat(bdir); erc == nil {
				mtime := finfo.ModTime()
				if now.IsZero() {
					now = time.Now()
				}
				if mtime.After(now) || now.Sub(mtime) < bidUnknownTTL {
					return fmt.Errorf("%s %q: unknown BID with %q age below ttl (%v)", op, bck, bdir, mtime)
				}
			}
		}

		mi.evictLomBucketCache(bck)
		dir := mi.makeDelPathBck(bck)
		if errMv := mi.MoveToDeleted(dir); errMv != nil {
			nlog.Errorf("%s %q: failed to rm dir %q: %v", op, bck, dir, errMv)
			// TODO: call fshc
		} else {
			n++
		}
	}
	if n < count {
		err = fmt.Errorf("%s %q: failed to destroy %d out of %d dirs", op, bck, count-n, count)
	}
	return
}

func RenameBucketDirs(bckFrom, bckTo *cmn.Bck) (err error) {
	avail := GetAvail()
	renamed := make([]*Mountpath, 0, len(avail))
	for _, mi := range avail {
		fromPath := mi.makeDelPathBck(bckFrom)
		toPath := mi.MakePathBck(bckTo)

		// remove destination bucket directory before renaming
		// (the operation will fail otherwise)
		errRm := RemoveAll(toPath)
		debug.AssertNoErr(errRm)

		if err = os.Rename(fromPath, toPath); err != nil {
			break
		}
		renamed = append(renamed, mi)
	}

	if err == nil {
		return
	}
	for _, mi := range renamed {
		fromPath := mi.MakePathBck(bckTo)
		toPath := mi.MakePathBck(bckFrom)
		if erd := os.Rename(fromPath, toPath); erd != nil {
			nlog.Errorln(erd)
		}
	}
	return
}

func moveMarkers(available MPI, from *Mountpath) {
	var (
		fromPath    = filepath.Join(from.Path, fname.MarkersDir)
		finfos, err = os.ReadDir(fromPath)
	)
	if err != nil {
		if !os.IsNotExist(err) {
			nlog.Errorf("Failed to read markers' dir %q: %v", fromPath, err)
		}
		return
	}
	if len(finfos) == 0 {
		return // no markers, nothing to do
	}

	// NOTE: `from` path must no longer be in the available mountpaths
	_, ok := available[from.Path]
	debug.Assert(!ok, from.String())
	for _, mi := range available {
		ok = true
		for _, fi := range finfos {
			debug.Assert(!fi.IsDir(), fname.MarkersDir+"/"+fi.Name()) // marker is file
			var (
				fromPath = filepath.Join(from.Path, fname.MarkersDir, fi.Name())
				toPath   = filepath.Join(mi.Path, fname.MarkersDir, fi.Name())
			)
			_, _, err := cos.CopyFile(fromPath, toPath, nil, cos.ChecksumNone)
			if err != nil && os.IsNotExist(err) {
				nlog.Errorf("Failed to move marker %q to %q: %v)", fromPath, toPath, err)
				ok = false
			}
		}
		if ok {
			break
		}
	}
	from.ClearMDs(true /*inclBMD*/)
}

// load node ID

// traverses all mountpaths to load and validate node ID
func LoadNodeID(mpaths cos.StrSet) (mDaeID string, err error) {
	for mp := range mpaths {
		daeID, err := _loadXattrID(mp)
		if err != nil {
			return "", err
		}
		if daeID == "" {
			continue
		}
		if mDaeID != "" {
			if mDaeID != daeID {
				return "", &ErrStorageIntegrity{
					Code: SieMpathIDMismatch,
					Msg:  fmt.Sprintf("target ID mismatch: %q vs %q(%q)", mDaeID, daeID, mp),
				}
			}
			continue
		}
		mDaeID = daeID
	}
	return
}

func _loadXattrID(mpath string) (daeID string, err error) {
	b, err := GetXattr(mpath, nodeXattrID)
	if err == nil {
		daeID = string(b)
		return
	}
	if cos.IsErrXattrNotFound(err) {
		err = nil
	}
	return
}

/////////
// MPI //
/////////

func (mpi MPI) String() string {
	return fmt.Sprintf("%v", mpi.toSlice())
}

func (mpi MPI) toSlice() []string {
	var (
		paths = make([]string, len(mpi))
		idx   int
	)
	for key := range mpi {
		paths[idx] = key
		idx++
	}
	return paths
}

//
// capacity management
//

func Cap() (cs CapStatus) {
	// config
	cs.OOS = ratomic.LoadInt64(&mfs.cs.OOS)
	cs.HighWM = ratomic.LoadInt64(&mfs.cs.HighWM)
	// metrics
	cs.TotalUsed = ratomic.LoadUint64(&mfs.cs.TotalUsed)
	cs.TotalAvail = ratomic.LoadUint64(&mfs.cs.TotalAvail)
	cs.PctMin = ratomic.LoadInt32(&mfs.cs.PctMin)
	cs.PctAvg = ratomic.LoadInt32(&mfs.cs.PctAvg)
	cs.PctMax = ratomic.LoadInt32(&mfs.cs.PctMax)
	return
}

func CapRefresh(config *cmn.Config, tcdf *TargetCDF) (cs CapStatus, err, errCap error) {
	var (
		c     Capacity
		avail = GetAvail()
	)
	if len(avail) == 0 {
		err = cmn.ErrNoMountpaths
		return
	}
	if config == nil {
		config = cmn.GCO.Get()
	}
	cs.HighWM, cs.OOS = config.Space.HighWM, config.Space.OOS
	cs.PctMin = 100
	for path, mi := range avail {
		if c, err = mi.getCapacity(config, true); err != nil {
			nlog.Errorf("%s: %v", mi, err)
			return
		}
		cs.TotalUsed += c.Used
		cs.TotalAvail += c.Avail
		cs.PctMax = max(cs.PctMax, c.PctUsed)
		cs.PctMin = min(cs.PctMin, c.PctUsed)
		cs.PctAvg += c.PctUsed
		if tcdf == nil {
			continue
		}
		cdf := tcdf.Mountpaths[path]
		if cdf == nil {
			tcdf.Mountpaths[path] = &CDF{}
			cdf = tcdf.Mountpaths[path]
		}
		cdf.Capacity = c
		cdf.Disks = mi.Disks
		cdf.FS = mi.FS.String()
	}
	cs.PctAvg /= int32(len(avail))
	errCap = cs.Err()

	if tcdf != nil {
		tcdf.PctMax, tcdf.PctAvg, tcdf.PctMin = cs.PctMax, cs.PctAvg, cs.PctMin
		if errCap != nil {
			tcdf.CsErr = errCap.Error()
		}
	}

	// update cached state
	ratomic.StoreInt64(&mfs.cs.HighWM, cs.HighWM)
	ratomic.StoreInt64(&mfs.cs.OOS, cs.OOS)
	ratomic.StoreUint64(&mfs.cs.TotalUsed, cs.TotalUsed)
	ratomic.StoreUint64(&mfs.cs.TotalAvail, cs.TotalAvail)
	ratomic.StoreInt32(&mfs.cs.PctMin, cs.PctMin)
	ratomic.StoreInt32(&mfs.cs.PctAvg, cs.PctAvg)
	ratomic.StoreInt32(&mfs.cs.PctMax, cs.PctMax)
	return
}

// called only and exclusively by `stats.Trunner` providing `config.Periodic.StatsTime` tick
func CapPeriodic(now int64, config *cmn.Config, tcdf *TargetCDF) (cs CapStatus, updated bool, err, errCap error) {
	if now < mfs.csExpires.Load() {
		return
	}
	cs, err, errCap = CapRefresh(config, tcdf)
	updated = err == nil
	mfs.csExpires.Store(now + int64(cs._next(config)))
	return
}

func CapStatusGetWhat() (fsInfo apc.CapacityInfo) {
	cs := Cap()
	fsInfo.Used = cs.TotalUsed
	fsInfo.Total = cs.TotalUsed + cs.TotalAvail
	fsInfo.PctUsed = float64(cs.PctAvg)
	return
}

///////////////
// CapStatus //
///////////////

// note: conditioning on max, not avg
func (cs *CapStatus) Err() (err error) {
	oos := cs.IsOOS()
	if oos || int64(cs.PctMax) > cs.HighWM {
		err = cmn.NewErrCapExceeded(cs.TotalUsed, cs.TotalAvail+cs.TotalUsed, cs.HighWM, 0 /*cleanup wm*/, cs.PctMax, oos)
	}
	return
}

func (cs *CapStatus) IsOOS() bool { return int64(cs.PctMax) > cs.OOS }

func (cs *CapStatus) IsNil() bool { return cs.TotalUsed == 0 && cs.TotalAvail == 0 }

func (cs *CapStatus) String() (s string) {
	var (
		totalUsed  = cos.ToSizeIEC(int64(cs.TotalUsed), 1)
		totalAvail = cos.ToSizeIEC(int64(cs.TotalAvail), 1)
	)
	s = fmt.Sprintf("cap(used %s, avail %s [min=%d%%, avg=%d%%, max=%d%%]", totalUsed, totalAvail,
		cs.PctMin, cs.PctAvg, cs.PctMax)
	switch {
	case cs.IsOOS():
		s += ", OOS"
	case int64(cs.PctMax) > cs.HighWM:
		s += ", high-wm"
	}
	s += ")"
	return
}

// next time to CapRefresh()
func (cs *CapStatus) _next(config *cmn.Config) time.Duration {
	var (
		util = int64(cs.PctMax)
		umin = min(config.Space.HighWM-10, config.Space.LowWM)
		umax = config.Space.OOS
		tmax = config.LRU.CapacityUpdTime.D()
		tmin = config.Periodic.StatsTime.D()
	)
	umin = min(umin, config.Space.CleanupWM)
	if util <= umin {
		return tmax
	}
	if util >= umax-1 {
		return tmin
	}
	ratio := (util - umin) * 100 / (umax - umin)
	return time.Duration(100-ratio)*(tmax-tmin)/100 + tmin
}
