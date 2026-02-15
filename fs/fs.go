// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

	onexxh "github.com/OneOfOne/xxhash"
)

const bidUnknownTTL = 2 * time.Minute // comment below; TODO: unify and move to config along w/ lom cache

const nodeXattrID = "user.ais.daemon_id"

// enum Mountpath.Flags
const (
	FlagBeingDisabled uint64 = 1 << iota
	FlagBeingDetached
	FlagDisabledByFSHC
	// media type
	flagRotational
	flagNVMe
	flagSlow // (unknown|fuse|overlay)
)

const FlagWaitingDD = FlagBeingDisabled | FlagBeingDetached

const (
	_unknown = ", unknown"
	_ssd     = ", ssd"
	_hdd     = ", hdd"
	_nvme    = ", nvme"
	_raid    = "/raid"
)

// Terminology:
// - a mountpath is equivalent to (configurable) fspath - both terms are used interchangeably;
// - each mountpath is, simply, a local directory that is serviced by a local filesystem;
// - there's a 1-to-1 relationship between a mountpath and a local filesystem
//   (different mountpaths map onto different filesystems, and vise versa);
// - mountpaths of the form <filesystem-mountpoint>/a/b/c are supported.

// filesystem Health Check
type HC interface {
	FSHC(err error, mi *Mountpath, fqn string)
	SoftFSHC()
}

type (
	Mountpath struct {
		LomCaches  cos.MultiHashMap // LOM caches
		info       string
		Path       string             // clean path
		Label      cos.MountpathLabel // (disk sharing; storage class; user-defined grouping)
		cos.FS                        // underlying filesystem
		Disks      []string           // owned disks (ios.FsDisks map => slice)
		flags      uint64             // bit flags (set/get atomic)
		PathDigest uint64             // (HRW logic)
		capacity   Capacity
	}
	MPI map[string]*Mountpath

	MFS struct {
		ios ios.IOS

		hc HC

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
		totalSize atomic.Uint64

		flags uint64

		mu sync.Mutex
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

var mfs *MFS // singleton (target only)

///////////////
// Mountpath //
///////////////

func NewMountpath(mpath string, label cos.MountpathLabel) (*Mountpath, error) {
	cleanMpath, err := cmn.ValidateMpath(mpath)
	if err != nil {
		return nil, err
	}
	if err = cos.Stat(cleanMpath); err != nil {
		return nil, cos.NewErrNotFound(nil, "mountpath "+mpath)
	}
	mi := &Mountpath{
		Path:       cleanMpath,
		Label:      label,
		PathDigest: onexxh.Checksum64S(cos.UnsafeB(cleanMpath), cos.MLCG32),
	}
	err = mi.resolveFS()
	return mi, err
}

func (mi *Mountpath) CheckFS() (err error) {
	dup := Mountpath{Path: mi.Path}
	if err = dup.resolveFS(); err != nil {
		err = fmt.Errorf("failed to resolve filesystem for the %s: %w", mi, err)
		return cmn.NewErrMpathCheck(err)
	}
	if !dup.FS.Equal(mi.FS) {
		err = fmt.Errorf("%s: detected filesystem change at runtime: %s => %s", mi, mi.FS.String(), dup.FS.String())
		return cmn.NewErrMpathCheck(err)
	}
	return nil
}

// flags
func (mi *Mountpath) SetFlags(flags uint64) { cos.SetFlag(&mi.flags, flags) }

func (mi *Mountpath) IsAnySet(flags uint64) bool { return cos.IsAnySetFlag(&mi.flags, flags) }
func (mi *Mountpath) IsRotational() bool         { return cos.IsAnySetFlag(&mi.flags, flagRotational) }
func (mi *Mountpath) IsNVMe() bool               { return cos.IsAnySetFlag(&mi.flags, flagNVMe) }

func (mi *Mountpath) String() string {
	lab := mi.Label.ToLog()
	if mi.info == "" {
		mediaTy := _unknown
		switch {
		case mi.IsRotational():
			mediaTy = _hdd
		case mi.IsNVMe():
			mediaTy = _nvme
		case len(mi.Disks) > 0:
			mediaTy = _ssd
		}
		switch len(mi.Disks) {
		case 0:
			if mi.FsType != "" && mi.FsType != mi.Fs {
				mi.info = fmt.Sprintf("mp[%s, fs=%s/%s%s%s]", mi.Path, mi.Fs, mi.FsType, lab, mediaTy)
			} else {
				mi.info = fmt.Sprintf("mp[%s, fs=%s%s%s]", mi.Path, mi.Fs, lab, mediaTy)
			}
		case 1:
			mi.info = fmt.Sprintf("mp[%s, %s%s%s]", mi.Path, mi.Disks[0], lab, mediaTy)
		default:
			mi.info = fmt.Sprintf("mp[%s, %v%s%s%s]", mi.Path, mi.Disks, lab, mediaTy, _raid)
		}
	}
	if !mi.IsAnySet(FlagWaitingDD) {
		return mi.info
	}
	l := len(mi.info)
	return mi.info[:l-1] + ", waiting-dd]"
}

func (mi *Mountpath) IsAvail() bool {
	avail := GetAvail()
	_, ok := avail[mi.Path]
	return ok
}

func (mi *Mountpath) CreateMissingBckDirs(bck *cmn.Bck) error {
	for contentType := range CSM.m {
		dir := mi.MakePathCT(bck, contentType)
		if err := cos.Stat(dir); err == nil {
			continue
		}
		if err := cos.CreateDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (mi *Mountpath) backupAtmost(from, backup string, bcnt, atMost int) (newBcnt int) {
	var (
		fromPath   = filepath.Join(mi.Path, from)
		backupPath = filepath.Join(mi.Path, backup)
	)
	os.Remove(backupPath)
	newBcnt = bcnt
	if bcnt >= atMost {
		return newBcnt
	}
	if err := cos.Stat(fromPath); err != nil {
		return newBcnt
	}
	if err := os.Rename(fromPath, backupPath); err != nil {
		nlog.Errorln(err)
		os.Remove(fromPath)
	} else {
		newBcnt = bcnt + 1
	}
	return newBcnt
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

// has-path method
// correctness is additionally enforced via cmn.IsNestedMpath() (to disallow nested mountpaths)
func (mi *Mountpath) HasPath(path string) bool {
	l := len(mi.Path)
	return len(path) > l && path[:l] == mi.Path && path[l] == filepath.Separator
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
	return buf
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
	debug.Assert(contentType != "")
	debug.Assert(objName != "")
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
				nlog.Errorf("Warning: %s bdir %s exists but local BMD is not the latest", bck.String(), dir)
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
				if contentType != WorkCT {
					return num, err
				}
				nlog.Errorln(err)
			}
		} else if err := cos.CreateDir(dir); err != nil {
			return num, fmt.Errorf("bucket %s: failed to create directory %s: %w", bck.String(), dir, err)
		}
		num++
	}
	return num, nil
}

func (mi *Mountpath) _setDisks(fsdisks ios.FsDisks) {
	mi.info = ""
	if len(fsdisks) == 0 {
		cos.ClrFlag(&mi.flags, flagRotational|flagNVMe)
		_ = mi.String()
		return
	}

	// (in re: hybrid raid)
	// mountpath is rotational if there's at least one hdd
	// mountpath is nvme iff all its disks are nvme
	var (
		set = flagNVMe
		clr = flagRotational
	)
	mi.info = ""
	mi.Disks = fsdisks.ToSlice()
	for _, info := range fsdisks {
		if info.Flags&ios.FlagRotational != 0 {
			set = flagRotational
			clr = flagNVMe
			break
		}
		if info.Flags&ios.FlagNVMe == 0 {
			clr |= flagNVMe
			set &^= flagNVMe
		}
	}
	pf := &mi.flags
	if set > 0 {
		cos.SetFlag(pf, set)
	}
	if clr > 0 && cos.IsAnySetFlag(pf, clr) {
		cos.ClrFlag(pf, clr)
	}
	_ = mi.String() // cache
}

// CapRefresh: available/used capacity
func (mi *Mountpath) getCapacity(config *cmn.Config, refresh bool) (c Capacity, err error) {
	if !refresh {
		c.Used = ratomic.LoadUint64(&mi.capacity.Used)
		c.Avail = ratomic.LoadUint64(&mi.capacity.Avail)
		c.PctUsed = ratomic.LoadInt32(&mi.capacity.PctUsed)
		return c, nil
	}
	statfs := &syscall.Statfs_t{}
	if err = syscall.Statfs(mi.Path, statfs); err != nil {
		mfs.hc.FSHC(err, mi, "")
		return c, err
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
	return c, nil
}

//
// mountpath add/enable helpers - always call under mfs lock
//

func (mi *Mountpath) AddEnabled(tid string, avail MPI, config *cmn.Config, blockDevs ios.BlockDevs) error {
	if err := mi._validate(avail, config); err != nil {
		return err
	}
	err := mi._addEnabled(tid, avail, config, blockDevs)
	if err == nil {
		mfs.fsIDs[mi.FsID] = mi.Path
	}
	cos.ClrFlag(&mi.flags, FlagWaitingDD|FlagDisabledByFSHC)
	return err
}

func (mi *Mountpath) AddDisabled(disabled MPI) {
	cos.ClrFlag(&mi.flags, FlagWaitingDD)
	disabled[mi.Path] = mi
	mfs.fsIDs[mi.FsID] = mi.Path
}

// check:
// - duplication
// - disk sharing
// - no disks
func (mi *Mountpath) _validate(avail MPI, config *cmn.Config) error {
	existingMi, ok := avail[mi.Path]
	if ok {
		return fmt.Errorf("duplicated mountpath %s (%s)", mi, existingMi)
	}
	otherMpath, ok := mfs.fsIDs[mi.FsID]
	if ok {
		if config.TestingEnv() {
			return nil
		}
		if !mi.Label.IsNil() {
			nlog.Warningf("FsID %v shared between (labeled) %s and %q - proceeding anyway", mi.FsID, mi, otherMpath)
			return nil
		}
		return fmt.Errorf("FsID %v: filesystem sharing is not allowed: %s vs %q", mi.FsID, mi, otherMpath)
	}
	// check nesting
	l := len(mi.Path)
	for mpath := range avail {
		if err := cmn.IsNestedMpath(mi.Path, l, mpath); err != nil {
			return err
		}
	}
	return nil
}

func (mi *Mountpath) _addEnabled(tid string, avail MPI, config *cmn.Config, blockDevs ios.BlockDevs) error {
	fsdisks, err := mfs.ios.AddMpath(mi.Path, mi.Fs, mi.Label, config, blockDevs)
	if err != nil {
		return err
	}
	if tid != "" && config.WritePolicy.MD != apc.WriteNever {
		if err := mi.SetDaemonIDXattr(tid); err != nil {
			return err
		}
	}
	mi._setDisks(fsdisks)
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
		return nil
	}

	// add new mp
	if err := mi._validate(avail, config); err != nil {
		return err
	}
	availableCopy := _cloneOne(avail)
	err = mi.AddEnabled(tid, availableCopy, config, nil /*blockDevs*/)
	if err == nil {
		putAvailMPI(availableCopy)
	}
	return err
}

func (mi *Mountpath) ClearDD() {
	cos.ClrFlag(&mi.flags, FlagWaitingDD)
}

func (mi *Mountpath) diskSize() (size uint64) {
	numBlocks, _, blockSize, err := ios.GetFSStats(mi.Path)
	if err != nil {
		nlog.Errorln(mi.String(), "total disk size err:", err, strings.Repeat("<", 50))
		mfs.hc.FSHC(err, mi, "")
	} else {
		size = numBlocks * uint64(blockSize)
	}
	return size
}

// Calculates on-disk size of bucket or bucket+prefix.
func (mi *Mountpath) onDiskSize(bck *cmn.Bck, prefix string) (uint64, error) {
	var (
		dirPath          string
		withNonDirPrefix bool
	)
	if prefix == "" {
		dirPath = mi.MakePathBck(bck)
	} else {
		dirPath = filepath.Join(mi.MakePathCT(bck, ObjCT), prefix)
		if cos.Stat(dirPath) != nil {
			withNonDirPrefix = true // ok to fail matching
		}
	}
	return ios.DirSizeOnDisk(dirPath, withNonDirPrefix)
}

func (mi *Mountpath) _cdf(tcdf *Tcdf) *CDF {
	cdf := tcdf.Mountpaths[mi.Path]
	if cdf == nil {
		tcdf.Mountpaths[mi.Path] = &CDF{}
		cdf = tcdf.Mountpaths[mi.Path]
	}
	cdf.Disks = mi.Disks
	cdf.FS = mi.FS
	cdf.Label = mi.Label
	cdf.Capacity = Capacity{} // reset (for caller to fill-in)
	return cdf
}

//nolint:staticcheck // making an exception for Warning
func (mi *Mountpath) RescanDisks() (warn, _ error) {
	res := mfs.ios.RescanDisks(mi.Path, mi.Fs, mi.Disks) // TODO: comments inside
	if res.Fatal != nil {
		return nil, res.Fatal
	}

	debug.Assert(len(res.FsDisks) > 0)
	if res.Attached == nil && res.Lost == nil {
		return nil, nil
	}
	debug.Assert(len(res.Attached)+len(res.Lost) > 0)

	if l := len(res.Attached); l > 0 {
		warn = fmt.Errorf("Warning: %s got new disk%s: %v", mi, cos.Plural(l), res.Attached)
		nlog.Errorln(warn)
	}
	if l := len(res.Lost); l > 0 {
		if warn != nil {
			warn = fmt.Errorf("Warning: %s lost disk%s: %v\n%v", mi, cos.Plural(l), res.Lost, warn)
		} else {
			warn = fmt.Errorf("Warning: %s lost disk%s: %v", mi, cos.Plural(l), res.Lost)
		}
	}

	// TODO: restart target and check its volume
	mi._setDisks(res.FsDisks)
	return warn, nil
}

// return mountpath alert (suffix)
// NOTE: the bits are not mutually exclusive; returning only one alert in the order of priority
func (mi *Mountpath) _alert(config *cmn.Config, c Capacity) string {
	flags := ratomic.LoadUint64(&mi.flags)
	switch {
	case (flags & FlagDisabledByFSHC) == FlagDisabledByFSHC:
		return DiskFault
	case c.PctUsed > int32(config.Space.OOS):
		return DiskOOS
	case (flags & FlagBeingDetached) == FlagBeingDetached:
		return Disk2Detach
	case (flags & FlagBeingDisabled) == FlagBeingDisabled:
		return Disk2Disable
	case c.PctUsed >= int32(config.Space.HighWM):
		return DiskHighWM
	}
	return ""
}

//
// MFS global
//

func New(fshc HC, num int) (blockDevs ios.BlockDevs) {
	mfs = &MFS{hc: fshc, fsIDs: make(map[cos.FsID]string, 10)}
	mfs.ios, blockDevs = ios.New(num)

	// init content spec mgr: reg content types and resolvers
	_once.Do(initCSM)

	return blockDevs
}

// used only in tests (NOTE: mfs.hc remains nil)
func TestNew(iostater ios.IOS) {
	const num = 10
	mfs = &MFS{fsIDs: make(map[cos.FsID]string, num)}
	if iostater == nil {
		mfs.ios, _ = ios.New(num)
	} else {
		mfs.ios = iostater
	}
	PutMPI(make(MPI, num), make(MPI, num))

	// ditto
	_once.Do(initCSM)
}

//
// disk utilizations (helpers)
//

func GetAllMpathUtils() (utils *ios.MpathUtil) { return mfs.ios.GetAllMpathUtils() }
func GetMpathUtil(mpath string) int64          { return mfs.ios.GetMpathUtil(mpath) }

// max disk utilization across mountpaths
func GetMaxUtil() (util int64) {
	var (
		utils = GetAllMpathUtils()
		avail = GetAvail()
	)
	for _, mi := range avail {
		if u := utils.Get(mi.Path); u > util {
			util = u
		}
	}
	return util
}

func (mi *Mountpath) GetUtil() int64 { return mfs.ios.GetMpathUtil(mi.Path) }

//
// more `ios` delegations
//

func putAvailMPI(avail MPI)    { mfs.available.Store(&avail) }
func putDisabMPI(disabled MPI) { mfs.disabled.Store(&disabled) }

func PutMPI(avail, disabled MPI) {
	putAvailMPI(avail)
	putDisabMPI(disabled)
}

func ToMPL() (mpl *apc.MountpathList) {
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
	return mpl
}

// NOTE: must be under mfs lock
func _cloneOne(mpis MPI) MPI { return maps.Clone(mpis) }

// cloneMPI returns a shallow copy of the current (available, disabled) mountpaths
func cloneMPI() (availableCopy, disabledCopy MPI) {
	avail, disabled := Get()
	availableCopy = _cloneOne(avail)
	disabledCopy = _cloneOne(disabled)
	return availableCopy, disabledCopy
}

// used only in tests (compare with AddMpath below)
func Add(mpath, tid string) (mi *Mountpath, err error) {
	mi, err = NewMountpath(mpath, cos.TestMpathLabel)
	if err != nil {
		return nil, err
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	mfs.mu.Unlock()
	return mi, err
}

// (via attach-mpath)
func AddMpath(tid, mpath string, label cos.MountpathLabel, cb func()) (mi *Mountpath, err error) {
	mi, err = NewMountpath(mpath, label)
	if err != nil {
		return nil, err
	}

	config := cmn.GCO.Get()
	if config.TestingEnv() {
		if err = config.LocalConfig.TestFSP.ValidateMpath(mi.Path); err != nil {
			nlog.Errorln(err, "- proceeding anyway")
		}
	}

	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()

	if mi.Path != mpath {
		nlog.Warningf("%s: clean path(%q) => %q", mi, mpath, mi.Path)
	}
	return mi, err
}

// (unit tests only - compare with EnableMpath below)
func Enable(mpath string) (mi *Mountpath, err error) {
	var cleanMpath string
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return nil, err
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	mi, err = enable(mpath, cleanMpath, "" /*tid*/, config)
	mfs.mu.Unlock()
	return mi, err
}

// Enable enables previously disabled mountpath. enabled is set to
// true if mountpath has been moved from disabled to available and exists is
// set to true if such mountpath even exists.
func EnableMpath(mpath, tid string, cb func()) (mi *Mountpath, err error) {
	var cleanMpath string
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return nil, err
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	mi, err = enable(mpath, cleanMpath, tid, config)
	if err == nil {
		cb()
	}
	mfs.mu.Unlock()
	return mi, err
}

func enable(mpath, cleanMpath, tid string, config *cmn.Config) (enabledMi *Mountpath, err error) {
	avail, disabled := Get()
	mi, ok := avail[cleanMpath]

	// dd-transition
	if ok {
		debug.Assert(cleanMpath == mi.Path)
		if _, ok = disabled[cleanMpath]; ok {
			err = fmt.Errorf("FATAL: %s vs (%s, %s)", mi, avail, disabled)
			nlog.Errorln(err)
			debug.AssertNoErr(err)
			return nil, err
		}
		if mi.IsAnySet(FlagWaitingDD) {
			availableCopy := _cloneOne(avail)
			mi, ok = availableCopy[cleanMpath]
			debug.Assert(ok)
			nlog.Warningln(mi.String()+":", "re-enabling during dd-transition")
			cos.ClrFlag(&mi.flags, FlagWaitingDD)
			enabledMi = mi
			putAvailMPI(availableCopy)
		} else if cmn.Rom.V(4, cos.ModFS) {
			nlog.Infof("%s: %s is already available, nothing to do", tid, mi)
		}
		return enabledMi, nil
	}

	// re-enable
	mi, ok = disabled[cleanMpath]
	if !ok {
		return nil, cmn.NewErrMpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
	}

	debug.Assert(cleanMpath == mi.Path)
	availableCopy, disabledCopy := cloneMPI()
	mi, ok = disabledCopy[cleanMpath]
	debug.Assert(ok)
	if err = mi.AddEnabled(tid, availableCopy, config, nil /*blockDevs*/); err != nil {
		return nil, err
	}

	enabledMi = mi
	delete(disabledCopy, cleanMpath)
	PutMPI(availableCopy, disabledCopy)
	return enabledMi, nil
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
			return nil, cmn.NewErrMpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
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
	_moveMarkers(availableCopy, mi)
	putAvailMPI(availableCopy)
	if availCnt > 0 && len(cb) > 0 {
		cb[0]()
	}
	return mi, nil
}

// begin (disable | detach) transaction: CoW-mark the corresponding mountpath
func BeginDD(action string, flags uint64, mpath string) (mi *Mountpath, numAvail int, alreadyDD bool, err error) {
	var cleanMpath string
	debug.Assert(cos.BitFlags(flags).IsAnySet(cos.BitFlags(FlagWaitingDD)))
	if cleanMpath, err = cmn.ValidateMpath(mpath); err != nil {
		return
	}
	mfs.mu.Lock()
	mi, numAvail, alreadyDD, err = begdd(action, flags, cleanMpath)
	mfs.mu.Unlock()
	return
}

// under lock
func begdd(action string, flags uint64, mpath string) (mi *Mountpath, numAvail int, alreadyDD bool, err error) {
	var (
		avail, disabled = Get()
		exists          bool
	)
	// dd inactive
	if _, exists = avail[mpath]; !exists {
		alreadyDD = true
		if mi, exists = disabled[mpath]; !exists {
			err = cmn.NewErrMpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
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
	mi.SetFlags(flags)
	putAvailMPI(clone)
	numAvail = len(clone) - 1
	return
}

// Disables a mountpath, i.e., removes it from usage but keeps in the volume
// (for possible future re-enablement). If successful, returns the disabled mountpath.
// Otherwise, returns nil (also in the case if the mountpath was already disabled).
func Disable(mpath string, cb ...func()) (*Mountpath, error) {
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
			return nil, err
		}
		availableCopy, disabledCopy := cloneMPI()
		cos.ClrFlag(&mi.flags, FlagWaitingDD)
		disabledCopy[cleanMpath] = mi

		config := cmn.GCO.Get()
		mfs.ios.RemoveMpath(cleanMpath, config.TestingEnv())
		delete(availableCopy, cleanMpath)
		delete(mfs.fsIDs, mi.FsID)
		_moveMarkers(availableCopy, mi)
		PutMPI(availableCopy, disabledCopy)
		if l := len(availableCopy); l == 0 {
			nlog.Errorf("disabled the last available mountpath %s", mi)
		} else {
			if len(cb) > 0 {
				cb[0]()
			}
			nlog.Infof("disabled mountpath %s (%d remain%s active)", mi, l, cos.Plural(l))
		}
		return mi, nil // return disabled mountpath
	}

	if _, ok := disabled[cleanMpath]; ok {
		return nil, nil // nothing to do
	}
	return nil, cmn.NewErrMpathNotFound(mpath, "" /*fqn*/, false /*disabled*/)
}

func _moveMarkers(avail MPI, from *Mountpath) {
	var (
		fromPath    = filepath.Join(from.Path, fname.MarkersDir)
		finfos, err = os.ReadDir(fromPath)
	)
	if err != nil {
		if !cos.IsNotExist(err) {
			nlog.Errorf("Failed to read markers' dir %q: %v", fromPath, err)
		}
		return
	}
	if len(finfos) == 0 {
		return // no markers, nothing to do
	}

	// `from` path must no longer be in _available_
	_, ok := avail[from.Path]
	debug.Assert(!ok, from.String())

	// copy + delete
	for _, mi := range avail {
		ok = true
		for _, fi := range finfos {
			debug.Assert(!fi.IsDir(), fname.MarkersDir+cos.PathSeparator+fi.Name()) // marker is a file
			var (
				fromPath = filepath.Join(from.Path, fname.MarkersDir, fi.Name())
				toPath   = filepath.Join(mi.Path, fname.MarkersDir, fi.Name())
			)
			_, _, err := cos.CopyFile(fromPath, toPath, nil, cos.ChecksumNone)
			if err != nil && !cos.IsNotExist(err) {
				nlog.Errorf("Failed to move marker %q to %q: %v", fromPath, toPath, err)
				mfs.hc.FSHC(err, mi, fromPath)
				ok = false
			}
		}
		if ok {
			break
		}
	}
	_ = from.clearMDs(true /*inclBMD*/)
}

//
// avail & disabled
//

func NumAvail() int {
	avail := GetAvail()
	return len(avail)
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

func getDisabled() MPI {
	disabled := mfs.disabled.Load()
	debug.Assert(disabled != nil)
	return *disabled
}

//
// buckets
//

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
	return errs
}

// NOTE: caller must evict LOM cache
func DestroyBucket(op string, bck *cmn.Bck, bid uint64) error {
	var (
		avail = GetAvail()
		now   time.Time
		n     int
	)
	for _, mi := range avail {
		// normally, unique bucket ID (aka BID) must be known
		// - i.e., non-zero (and unique);
		// zero ID indicates that either we are in the middle of bucket
		// creation OR the latter was interrupted (and txn-create aborted) -
		// thus, prior to going ahead with deletion:
		if bid == 0 {
			bdir := mi.MakePathBck(bck)
			if finfo, erc := os.Lstat(bdir); erc == nil {
				mtime := finfo.ModTime()
				if now.IsZero() {
					now = time.Now()
				}
				if mtime.After(now) || now.Sub(mtime) < bidUnknownTTL {
					return fmt.Errorf("%s %q: unknown BID with %q age below ttl (%v)", op, bck.String(), bdir, mtime)
				}
			}
		}

		dir := mi.makeDelPathBck(bck)
		if errMv := mi.MoveToDeleted(dir); errMv != nil {
			nlog.Errorf("%s %q: failed to rm dir %q: %v", op, bck.String(), dir, errMv)
			mfs.hc.FSHC(errMv, mi, "")
		} else {
			n++
		}
	}

	if count := len(avail); n < count {
		return fmt.Errorf("%s %q: failed to destroy %d out of %d dirs", op, bck.String(), count-n, count)
	}
	return nil
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
			mfs.hc.FSHC(erd, mi, "")
		}
	}
	return
}

//
// load node ID - traverses all mountpaths to load and validate
//

func LoadNodeID(mpaths cos.StrKVs) (mDaeID string, err error) {
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
	} else {
		err = fmt.Errorf("unexpected failure to access mountpath %q: %w", mpath, err)
	}
	return
}

//
// capacity management/reporting
//

const (
	volSizeMin  = 10 * cos.MiB
	volSizeZero = "zero-size volume"
)

// set media type and volume size - all mountpaths (all disks)
// note: normally, mountpaths do not share underlying devices;
// `seen` map to handle specific exceptions:
// - local playground
// - labeled mountpaths (or "mountpath labels")
func SetVolSizeMedia() error {
	var (
		mediaTy string
		totalSz uint64
		flags   = flagNVMe
		seen    = make(map[cos.FsID]struct{}, len(mfs.fsIDs))
		avail   = GetAvail()
		cnt     int
	)
	for _, mi := range avail {
		if mi.IsAnySet(FlagWaitingDD) {
			continue
		}
		cnt++
		if _, ok := seen[mi.FsID]; ok {
			continue
		}
		seen[mi.FsID] = struct{}{}

		// detect (slow) fuse and overlay
		// TODO:
		// - consider factoring-in len(mi.Disks) == 0
		// - and vice versa, len(mi.Disks) > 1 may indicate "fast" for HDDs
		if strings.Contains(mi.FsType, "fuse") || mi.FsType == "overlay" || mi.FsType == "aufs" {
			flags |= flagSlow
		}

		if mi.IsRotational() {
			flags = flagRotational
			mediaTy = _hdd
		} else if !mi.IsNVMe() && flags != flagRotational {
			flags = 0
		}
		totalSz += mi.diskSize()
	}
	mfs.totalSize.Store(totalSz)

	// log & flags
	if cnt == 0 {
		ratomic.StoreUint64(&mfs.flags, 0)
		return cmn.ErrNoMountpaths
	}
	if totalSz == 0 {
		return fmt.Errorf("%s (mountpaths: %d/%d)", volSizeZero, cnt, len(avail))
	}
	ratomic.StoreUint64(&mfs.flags, flags)
	if mediaTy == "" {
		mediaTy = cos.Ternary(flags == flagNVMe, _nvme, _ssd)
	}
	if flags&flagSlow != 0 {
		mediaTy += "(slow)"
	}
	nlog.Infof("volume: [%s%s]", cos.ToSizeIEC(int64(totalSz), 2), mediaTy)
	return nil
}

func GetVolSize() (volSize uint64, err error) {
	if volSize = mfs.totalSize.Load(); volSize < volSizeMin {
		if volSize == 0 {
			err = errors.New(volSizeZero)
		} else {
			err = fmt.Errorf("volume size (%d bytes) below expected minimum (%s)", volSize, cos.IEC(volSizeMin, 0))
			if cmn.Rom.TestingEnv() {
				nlog.Warningln(err)
				err = nil
			}
		}
	}
	return
}

func IsRotational() bool { return cos.IsAnySetFlag(&mfs.flags, flagRotational) }
func IsNVMe() bool       { return cos.IsAnySetFlag(&mfs.flags, flagNVMe) }
func IsSlow() bool       { return cos.IsAnySetFlag(&mfs.flags, flagSlow) }

// bucket and bucket+prefix on-disk sizing
func OnDiskSize(bck *cmn.Bck, prefix string) (size uint64) {
	avail := GetAvail()
	for _, mi := range avail {
		if mi.IsAnySet(FlagWaitingDD) {
			continue
		}
		sz, err := mi.onDiskSize(bck, prefix)
		if err != nil {
			if cmn.Rom.V(4, cos.ModFS) {
				nlog.Warningln("failed to calculate size on disk:", err, "[", mi.String(), bck.String(), prefix, "]")
			}
			return 0
		}
		size += sz
	}
	return
}

// via (`apc.WhatDiskStats`, target_stats)
func DiskStats(allds cos.AllDiskStats, tcdf *Tcdf, config *cmn.Config, refreshCap bool) {
	// iops and bw
	mfs.ios.DiskStats(allds)

	if !refreshCap {
		if tcdf == nil {
			return
		}
		debug.Assert(false)
	}

	// cos.AllDiskStats <= alert suffixex, if any
	avail := GetAvail()
	for _, mi := range avail {
		var a string // alert suffix
		c, err := mi.getCapacity(config, true /*refresh*/)

		if err != nil {
			err = cmn.NewErrGetCap(err)
			nlog.Errorln(mi.String(), err)
			a = "(" + err.Error() + ")" // unlikely
		} else {
			a = mi._alert(config, c)

			if tcdf != nil {
				cdf := mi._cdf(tcdf)
				cdf.Capacity = c
			}
		}
		if a == "" {
			continue
		}
		for _, d := range mi.Disks {
			if dstats, ok := allds[d]; ok {
				// [convention] alert name suffix: <DISK NAME>[(alert)]
				delete(allds, d)
				allds[d+a] = dstats
			}
		}
	}
}

//
// cap status: get, refresh, periodic
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

func NoneShared(numMpaths int) bool { return len(mfs.fsIDs) >= numMpaths }

// sum up && compute %% capacities while skipping already _counted_ filesystems
func CapRefresh(config *cmn.Config, tcdf *Tcdf) (cs CapStatus, _, errCap error) {
	var (
		fsIDs  []cos.FsID
		avail  = GetAvail()
		l      = len(avail)
		n      int // num different filesystems (<= len(mfs.fsIDs))
		unique bool
	)
	if l == 0 {
		if tcdf != nil {
			tcdf.Mountpaths = make(map[string]*CDF)
		}
		return cs, cmn.ErrNoMountpaths, nil
	}

	// fast path: available w/ no sharing
	fast := NoneShared(l)
	unique = fast

	if !fast {
		fsIDs = make([]cos.FsID, 0, l)
	}

	cs.HighWM, cs.OOS = config.Space.HighWM, config.Space.OOS
	cs.PctMin = 101
	for _, mi := range avail {
		if !fast {
			fsIDs, unique = cos.AddUniqueFsID(fsIDs, mi.FsID)
		}
		if !unique {
			// (same fs across)
			if tcdf != nil {
				_ = mi._cdf(tcdf)
			}
			continue
		}

		// this mountpath's cap
		c, err := mi.getCapacity(config, true)
		if err != nil {
			nlog.Errorln(mi.String()+":", err)
			mfs.hc.FSHC(err, mi, "")
			return cs, err, nil
		}
		if tcdf != nil {
			cdf := mi._cdf(tcdf)
			cdf.Capacity = c

			if a := mi._alert(config, c); a != "" {
				// [convention] alert name suffix: <DISK NAME>[(alert)]
				cdf._alert(a)
			}
		}

		// recompute totals
		cs.TotalUsed += c.Used
		cs.TotalAvail += c.Avail
		cs.PctMax = max(cs.PctMax, c.PctUsed)
		cs.PctMin = min(cs.PctMin, c.PctUsed)
		n++
		cs.PctAvg += c.PctUsed
	}
	debug.Assert(cs.PctMin < 101)
	cs.PctAvg /= int32(n)

	errCap = cs.Err()

	// fill-in and prune
	if tcdf != nil {
		tcdf.PctMax, tcdf.PctAvg, tcdf.PctMin = cs.PctMax, cs.PctAvg, cs.PctMin
		tcdf.TotalUsed, tcdf.TotalAvail = cs.TotalUsed, cs.TotalAvail
		if errCap != nil {
			tcdf.CsErr = errCap.Error()
		}
		// prune detached and disabled, if any
		for mpath := range tcdf.Mountpaths {
			if _, ok := avail[mpath]; !ok {
				delete(tcdf.Mountpaths, mpath)
			}
		}
		// duplicate shared filesystem cap => (its mountpaths)
		if n < l {
			for mpath1, cdf1 := range tcdf.Mountpaths {
				for mpath2, cdf2 := range tcdf.Mountpaths {
					if mpath1 != mpath2 && cdf1.FS.Equal(cdf2.FS) {
						_either(cdf1, cdf2)
					}
				}
			}
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

	return cs, nil, errCap
}

func _either(cdf1, cdf2 *CDF) {
	if cdf1.Capacity.Used == 0 && cdf1.Capacity.Avail == 0 {
		cdf1.Capacity = cdf2.Capacity
	} else if cdf2.Capacity.Used == 0 && cdf2.Capacity.Avail == 0 {
		cdf2.Capacity = cdf1.Capacity
	}
}

func ExpireCapCache() { mfs.csExpires.Store(0) } // upon any change in config.space

// called only and exclusively by `stats.Trunner` providing `config.Periodic.StatsTime` tick
func CapPeriodic(now int64, config *cmn.Config, tcdf *Tcdf, flags cos.NodeStateFlags) (cs CapStatus, updated bool, err, errCap error) {
	const (
		mask = cos.DiskFault | cos.DiskOOS | cos.DiskLowCapacity | cos.OOS | cos.LowCapacity
	)
	if (flags&mask) == 0 && now < mfs.csExpires.Load() {
		cs = Cap()
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
func (cs *CapStatus) Err() error {
	testing := cmn.Rom.TestingEnv()
once:
	oos := cs.IsOOS()
	debug.Assertf(!oos || int64(cs.PctMax) > cs.HighWM, "oos %t vs (%d, %d)", oos, cs.PctMax, cs.HighWM)

	switch {
	case oos:
		return cs.emit(oos)
	case int64(cs.PctMax) <= cs.HighWM:
		return nil
	case !testing:
		return cs.emit(oos)
	}

	//
	// integration testing env-s may be severely constrained
	//
	avail := GetAvail()
	for _, mi := range avail {
		mi.RemoveDeleted("cap-err-testing-env")
	}

	// recompute capacity directly (cannot call CapRefresh())
	config := cmn.GCO.Get()
	var pctMax int32
	for _, mi := range avail {
		c, err := mi.getCapacity(config, true /*refresh*/)
		if err != nil {
			continue
		}
		pctMax = max(pctMax, c.PctUsed)
	}
	if int64(pctMax) <= config.Space.HighWM {
		nlog.Infoln("cap: stale", cs.PctMax, "=> actual", pctMax)
		return nil
	}

	testing = false
	goto once
}

func (cs *CapStatus) emit(oos bool) error {
	return cmn.NewErrCapExceeded(cs.TotalUsed, cs.TotalAvail+cs.TotalUsed, cs.HighWM, 0 /*cleanup wm*/, cs.PctMax, oos)
}

func (cs *CapStatus) IsOOS() bool { return int64(cs.PctMax) > cs.OOS }

func (cs *CapStatus) IsNil() bool { return cs.TotalUsed == 0 && cs.TotalAvail == 0 }

func (cs *CapStatus) String() string {
	var (
		sb         cos.SB
		totalUsed  = cos.IEC(int64(cs.TotalUsed), 1)
		totalAvail = cos.IEC(int64(cs.TotalAvail), 1)
	)
	sb.Init(80)
	sb.WriteString("cap(used ")
	sb.WriteString(totalUsed)
	sb.WriteString(", avail ")
	sb.WriteString(totalAvail)
	sb.WriteString(" [min=")
	sb.WriteString(strconv.Itoa(int(cs.PctMin)))
	sb.WriteString("%, avg=")
	sb.WriteString(strconv.Itoa(int(cs.PctAvg)))
	sb.WriteString("%, max=")
	sb.WriteString(strconv.Itoa(int(cs.PctMax)))
	sb.WriteUint8(']')

	switch {
	case cs.IsOOS():
		sb.WriteString(", OOS")
	case int64(cs.PctMax) > cs.HighWM:
		sb.WriteString(", high-wm")
	}
	sb.WriteUint8(')')
	return sb.String()
}

// next time to CapRefresh()
func (cs *CapStatus) _next(config *cmn.Config) time.Duration {
	var (
		util = int64(cs.PctMax)
		umin = min(config.Space.LowWM-5, config.Space.CleanupWM)
		umax = min(config.Space.OOS-5, config.Space.HighWM)
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
