// Package volume provides the volume abstraction and methods to bootstrap, store with redundancy,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"fmt"
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

func (vmd *VMD) addMountpath(mi *fs.MountpathInfo, enabled bool) {
	vmd.Mountpaths[mi.Path] = &fsMpathMD{
		Path:    mi.Path,
		Enabled: enabled,
		Fs:      mi.Fs,
		FsType:  mi.FsType,
		FsID:    mi.FsID,
	}
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
		glog.Errorf("cannot store VMD: %v", cmn.ErrNoMountpaths)
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
