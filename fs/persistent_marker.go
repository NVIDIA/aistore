// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

func MarkerExists(marker string) bool {
	availableMpaths, _ := Get()
	for _, mp := range availableMpaths {
		path := filepath.Join(mp.Path, marker)
		if err := Access(path); err == nil {
			return true
		}
	}
	return false
}

func PutMarker(marker string) error {
	var (
		mpath *MountpathInfo
	)
	availableMpaths, _ := Get()
	// get random mountpath
	for _, mp := range availableMpaths {
		mpath = mp
		break
	}
	if mpath == nil {
		return fmt.Errorf("could not create %q marker: %s", marker, cmn.NoMountpaths)
	}

	path := filepath.Join(mpath.Path, marker)
	_, err := cmn.CreateFile(path)
	return err
}

func RemoveMarker(marker string) error {
	var (
		err     error
		deleted bool

		availableMpaths, _ = Get()
	)
	for _, mp := range availableMpaths {
		path := filepath.Join(mp.Path, marker)
		if errRm := os.Remove(path); errRm != nil && !os.IsNotExist(errRm) {
			glog.Errorf("failed to cleanup %q marker from %s: %v", marker, mp.Path, err)
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
