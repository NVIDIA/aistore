// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/fs"
)

const vmdCopies = 3

type (
	fsMpathMD struct {
		Path    string      `json:"mountpath"`
		Fs      string      `json:"fs"`
		FsType  string      `json:"fs_type"`
		FsID    cos.FsID    `json:"fs_id"`
		Ext     interface{} `json:"ext,omitempty"` // reserved for within-metaversion extensions
		Enabled bool        `json:"enabled"`
	}

	// VMD is AIS target's volume metadata structure
	VMD struct {
		Version    uint64                `json:"version,string"` // version inc-s upon mountpath add/remove, etc.
		Mountpaths map[string]*fsMpathMD `json:"mountpaths"`     // mountpath => details
		DaemonID   string                `json:"daemon_id"`      // this target node ID
		// private
		cksum *cos.Cksum // VMD checksum
		info  string
	}
)

//
// bootstrap and init
//

func BootstrapVMD(tid string, configPaths cos.StringSet) (vmd *VMD, err error) {
	if len(configPaths) == 0 {
		err = fmt.Errorf("no fspaths - see README => Configuration and fspaths section in the config.sh")
		return
	}
	available := make(fs.MPI, len(configPaths)) // strictly to satisfy LoadVMD (below)
	for mpath := range configPaths {
		available[mpath] = nil
	}
	return loadVMD(tid, available)
}

// bootstrap mountpaths and VMD (the `volume`)
func InitNoVMD(tid string, config *cmn.Config) (err error) {
	var (
		configPaths    = config.FSpaths.Paths
		availablePaths = make(fs.MPI, len(configPaths))
		disabledPaths  = make(fs.MPI)
	)
	for path := range configPaths {
		var mi *fs.MountpathInfo
		if mi, err = fs.NewMountpath(path, tid); err != nil {
			return
		}
		if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
			return
		}
		if len(mi.Disks) == 0 && !config.TestingEnv() {
			err = &fs.ErrMpathNoDisks{Mi: mi}
			return
		}
	}
	fs.PutMpaths(availablePaths, disabledPaths)
	return
}

func InitVMD(tid string, vmd *VMD) (changed bool, err error) {
	var (
		config         = cmn.GCO.Get()
		availablePaths = make(fs.MPI, len(vmd.Mountpaths))
		disabledPaths  = make(fs.MPI)
	)
	debug.Assert(vmd.DaemonID == tid)

	for mpath, fsMpathMD := range vmd.Mountpaths {
		var mi *fs.MountpathInfo
		mi, err = fs.NewMountpath(mpath, tid)
		if fsMpathMD.Enabled {
			if err != nil {
				glog.Error(err) // TODO: remove redundant glog (here and elsewhere)
				return
			}
			if mi.Path != mpath {
				glog.Warningf("%s: cleanpath(%q) => %q", mi, mpath, mi.Path)
			}
			if mi.Fs != fsMpathMD.Fs || mi.FsType != fsMpathMD.FsType || mi.FsID != fsMpathMD.FsID {
				err = &fs.ErrStorageIntegrity{
					Code: fs.SieFsDiffers,
					Msg: fmt.Sprintf("Warning: filesystem change detected: %+v vs %+v (is it benign?)",
						mi.FilesystemInfo, fsMpathMD),
				}
				glog.Error(err)
			}
			if err = mi.AddEnabled(tid, availablePaths, config); err != nil {
				glog.Error(err)
				return
			}
			if len(mi.Disks) == 0 && !config.TestingEnv() {
				err = &fs.ErrMpathNoDisks{Mi: mi}
				glog.Errorf("Warning: %v", err)
			}
		} else {
			mi.Fs = fsMpathMD.Fs
			mi.FsType = fsMpathMD.FsType
			mi.FsID = fsMpathMD.FsID
			mi.AddDisabled(disabledPaths)
		}
	}

	fs.PutMpaths(availablePaths, disabledPaths)

	if la, lc := len(availablePaths), len(config.FSpaths.Paths); la != lc {
		glog.Warningf("number of available mountpaths (%d) differs from the configured (%d)", la, lc)
		glog.Warningln("run 'ais storage mountpath [attach|detach]', fix the config, or ignore")
	}
	return
}

func CreateVMD(tid string) (vmd *VMD, err error) {
	var (
		curVersion          uint64
		available, disabled = fs.Get()
	)
	// Try to load the currently stored vmd to determine the version.
	vmd, err = loadVMD(tid, nil)
	if err != nil {
		glog.Warning(err) // TODO: handle
		err = nil
	}
	if vmd != nil {
		curVersion = vmd.Version
	}

	vmd = newVMD(len(available))
	vmd.DaemonID = tid
	vmd.Version = curVersion + 1 // Bump the version.

	addMountpath := func(mpath *fs.MountpathInfo, enabled bool) {
		vmd.Mountpaths[mpath.Path] = &fsMpathMD{
			Path:    mpath.Path,
			Enabled: enabled,
			Fs:      mpath.Fs,
			FsType:  mpath.FsType,
			FsID:    mpath.FsID,
		}
	}
	for _, mpath := range available {
		addMountpath(mpath, true /*enabled*/)
	}
	for _, mpath := range disabled {
		addMountpath(mpath, false /*enabled*/)
	}
	err = vmd.persist()
	return
}

func LoadVMDTest() (*VMD, error) { return loadVMD("", nil) }

// loadVMD discovers, loads, and validates the most recently updated VMD (which is stored
// in several copies for fredundancy).
// - Returns nil if VMD does not exist
// - Returns error on failure to validate or load existing VMD
func loadVMD(tid string, available fs.MPI) (vmd *VMD, err error) {
	if available == nil {
		available, _ = fs.Get()
	}
	l := len(available)
	for mpath := range available {
		var v *VMD
		v, err = loadOneVMD(tid, vmd, mpath, l)
		if err != nil {
			return
		}
		if v != nil {
			vmd = v
		}
	}
	return
}

// given mountpath return a greater-version VMD if available
func loadOneVMD(tid string, vmd *VMD, mpath string, l int) (*VMD, error) {
	var (
		v   = newVMD(l)
		err = v.load(mpath)
	)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, &fs.ErrStorageIntegrity{
			Code: fs.SieMetaCorrupted,
			Msg:  fmt.Sprintf("failed to load VMD from %q: %v", mpath, err),
		}
	}
	if vmd == nil {
		if tid != "" && v.DaemonID != tid {
			return nil, &fs.ErrStorageIntegrity{
				Code: fs.SieTargetIDMismatch,
				Msg:  fmt.Sprintf("%s has a different target ID: %q != %q", v, v.DaemonID, tid),
			}
		}
		return v, nil
	}
	//
	// validate
	//
	debug.Assert(vmd.DaemonID == tid || tid == "")
	if v.DaemonID != vmd.DaemonID {
		return nil, &fs.ErrStorageIntegrity{
			Code: fs.SieTargetIDMismatch,
			Msg:  fmt.Sprintf("%s has a different target ID: %q != %q", v, v.DaemonID, vmd.DaemonID),
		}
	}
	if v.Version > vmd.Version {
		if !_mpathGreaterEq(v, vmd, mpath) {
			glog.Warningf("mpath %s: VMD version mismatch: %s vs %s", mpath, v, vmd)
		}
		return v, nil
	}
	if v.Version < vmd.Version {
		if !_mpathGreaterEq(vmd, v, mpath) {
			glog.Warningf("mpath %s: VMD version mismatch: %s vs %s", mpath, vmd, v)
		}
	} else if !v.equal(vmd) { // NOTE: same version must be identical
		err = &fs.ErrStorageIntegrity{
			Code: fs.SieNotEqVMD,
			Msg:  fmt.Sprintf("VMD differs: %s vs %s (%q)", vmd, v, mpath),
		}
	}
	return nil, err
}

func _mpathGreaterEq(curr, prev *VMD, mpath string) bool {
	currMd, currOk := curr.Mountpaths[mpath]
	prevMd, prevOk := prev.Mountpaths[mpath]
	if !currOk {
		return false
	} else if !prevOk {
		return true
	} else if currMd.Enabled {
		return true
	} else if currMd.Enabled == prevMd.Enabled {
		return true
	}
	return false
}

/////////
// VMD //
/////////

// interface guard
var _ jsp.Opts = (*VMD)(nil)

func (*VMD) JspOpts() jsp.Options { return jsp.CCSign(cmn.MetaverVMD) }

func newVMD(expectedSize int) *VMD {
	return &VMD{Mountpaths: make(map[string]*fsMpathMD, expectedSize)}
}

func (vmd *VMD) load(mpath string) (err error) {
	fpath := filepath.Join(mpath, cmn.VmdFname)
	if vmd.cksum, err = jsp.LoadMeta(fpath, vmd); err != nil {
		return err
	}
	if vmd.DaemonID == "" {
		debug.Assert(false) // Cannot happen in normal environment.
		return fmt.Errorf("daemon id is empty for vmd on %q", mpath)
	}
	return nil
}

func (vmd *VMD) persist() (err error) {
	cnt, availCnt := fs.PersistOnMpaths(cmn.VmdFname, "", vmd, vmdCopies, nil, nil /*wto*/)
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		glog.Errorf("cannot store VMD: %v", fs.ErrNoMountpaths)
		return
	}
	return fmt.Errorf("failed to store VMD on any of the mountpaths (%d)", availCnt)
}

func (vmd *VMD) equal(other *VMD) bool {
	debug.Assert(vmd.cksum != nil)
	debug.Assert(other.cksum != nil)
	return vmd.DaemonID == other.DaemonID &&
		vmd.Version == other.Version &&
		vmd.cksum.Equal(other.cksum)
}

func (vmd *VMD) String() string {
	if vmd.info != "" {
		return vmd.info
	}
	return vmd._string()
}

func (vmd *VMD) _string() string {
	mps := make([]string, len(vmd.Mountpaths))
	i := 0
	for mpath, md := range vmd.Mountpaths {
		mps[i] = mpath
		if !md.Enabled {
			mps[i] += "(-)"
		}
		i++
	}
	return fmt.Sprintf("VMD v%d(%s, %v)", vmd.Version, vmd.DaemonID, mps)
}
