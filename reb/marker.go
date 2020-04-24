// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	rebalanceMarker = ".rebalance_marker"
	resilverMarker  = ".resilver_marker"
)

func MarkerExists(kind string) bool {
	avail, _ := fs.Mountpaths.Get()
	for _, mp := range avail {
		path := makeMarkerPath(mp.Path, kind)
		if err := fs.Access(path); err == nil {
			return true
		}
	}
	return false
}

func IsRebalancing(kind string) (aborted, running bool) {
	cmn.Assert(kind == cmn.ActRebalance || kind == cmn.ActResilver)
	if MarkerExists(kind) {
		aborted = true
	}
	running = xaction.Registry.IsXactRunning(xaction.XactQuery{Kind: kind})
	if !running {
		return
	}
	aborted = false
	return
}

// persistent mark indicating rebalancing in progress
func makeMarkerPath(path, kind string) (pm string) {
	switch kind {
	case cmn.ActResilver:
		pm = filepath.Join(path, resilverMarker)
	case cmn.ActRebalance:
		pm = filepath.Join(path, rebalanceMarker)
	default:
		cmn.Assert(false)
	}
	return
}

func putMarker(kind string) error {
	var (
		mpath *fs.MountpathInfo
	)
	avail, _ := fs.Mountpaths.Get()
	// get random mountpath
	for _, mp := range avail {
		mpath = mp
		break
	}
	if mpath == nil {
		return errors.New("could not create resilver/rebalance marker: no mountpaths")
	}

	path := makeMarkerPath(mpath.Path, kind)
	_, err := cmn.CreateFile(path)
	return err
}

func removeMarker(kind string) error {
	var err error
	avail, _ := fs.Mountpaths.Get()
	deleted := false
	for _, mp := range avail {
		path := makeMarkerPath(mp.Path, kind)
		if errRm := os.Remove(path); errRm != nil && !os.IsNotExist(errRm) {
			glog.Errorf("failed to cleanup rebalance %s marker from %s: %v",
				kind, mp.Path, err)
			err = errRm
		} else if errRm == nil {
			deleted = true
		}
	}
	if err == nil && !deleted {
		err = errors.New("marker not found on any mpath")
	}
	return err
}
