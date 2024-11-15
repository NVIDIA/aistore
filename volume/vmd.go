// Package volume provides volume (a.k.a. pool of disks) abstraction and methods to configure, store,
// and validate the corresponding metadata. AIS volume is built on top of mountpaths (fs package).
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package volume

import (
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
)

const vmdCopies = 3

type (
	fsMpathMD struct {
		Ext     any       `json:"ext,omitempty"` // reserved for within-metaversion extensions
		Path    string    `json:"mountpath"`
		Label   ios.Label `json:"mountpath_label"`
		Fs      string    `json:"fs"`
		FsType  string    `json:"fs_type"`
		FsID    cos.FsID  `json:"fs_id"`
		Enabled bool      `json:"enabled"`
	}

	// VMD is AIS target's volume metadata structure
	VMD struct {
		Mountpaths map[string]*fsMpathMD `json:"mountpaths"` // mountpath => details
		cksum      *cos.Cksum            // content checksum
		DaemonID   string                `json:"daemon_id"` // this target node ID
		info       string                // String() only once
		Version    uint64                `json:"version,string"` // version inc-s upon mountpath add/remove, etc.
	}
)

func _mpathGreaterEq(curr, prev *VMD, mpath string) bool {
	currMd, currOk := curr.Mountpaths[mpath]
	prevMd, prevOk := prev.Mountpaths[mpath]
	switch {
	case !currOk:
		return false
	case !prevOk:
		return true
	case currMd.Enabled:
		return true
	case currMd.Enabled == prevMd.Enabled:
		return true
	}
	return false
}

/////////
// VMD //
/////////

// interface guard
var _ jsp.Opts = (*VMD)(nil)

func (*VMD) JspOpts() jsp.Options {
	opts := jsp.CCSign(cmn.MetaverVMD)
	opts.OldMetaverOk = 1
	return opts
}

func (vmd *VMD) addMountpath(mi *fs.Mountpath, enabled bool) {
	vmd.Mountpaths[mi.Path] = &fsMpathMD{
		Path:    mi.Path,
		Label:   mi.Label,
		Fs:      mi.Fs,
		FsType:  mi.FsType,
		FsID:    mi.FsID,
		Enabled: enabled,
	}
}

func (vmd *VMD) load(mpath string) (err error) {
	fpath := filepath.Join(mpath, fname.Vmd)
	if vmd.cksum, err = jsp.LoadMeta(fpath, vmd); err != nil {
		return
	}
	if vmd.DaemonID == "" {
		debug.Assert(false) // cannot happen
		err = fmt.Errorf("target ID is empty for vmd on %q", mpath)
	}
	return
}

func (vmd *VMD) persist() (err error) {
	cnt, availCnt := fs.PersistOnMpaths(fname.Vmd, "", vmd, vmdCopies, nil, nil /*wto*/)
	if cnt > 0 {
		return
	}
	if availCnt == 0 {
		nlog.Errorf("cannot store VMD: %v", cmn.ErrNoMountpaths)
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
